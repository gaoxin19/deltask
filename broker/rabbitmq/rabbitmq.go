// Package rabbitmq 提供了 Deltask 的 RabbitMQ Broker 实现
// 利用 rabbitmq-delayed-message-exchange 插件来支持延迟任务
package rabbitmq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/gaoxin19/deltask/broker"
	"github.com/gaoxin19/deltask/logger"
	"github.com/gaoxin19/deltask/task"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
)

const (
	delayedExchangeType    = "x-delayed-message"
	defaultPublishPoolSize = 100
)

var (
	ErrBrokerNotConnected = errors.New("broker is not connected")
	ErrBrokerClosed       = errors.New("broker is closed")
)

// Config 封装了连接 RabbitMQ 所需的配置
type Config struct {
	URL       string // AMQP 连接 URL
	Namespace string // 用于隔离资源的命名空间
	// Prefetch 是消费端 QoS 预取数量
	Prefetch int
	// PublishPoolSize 发布消息时使用的 Channel 池大小。决定了最大并发发布数量。如果设置为 0，将使用默认值
	PublishPoolSize int
}

// rabbitBroker 实现了 broker.Broker 接口
type rabbitBroker struct {
	config Config
	logger *logger.Logger
	mu     sync.RWMutex // 保护 conn 和 isClosed 状态

	conn        *amqp.Connection
	publishPool chan *amqp.Channel

	delayedExchangeName string
	isClosed            bool
	reconnectStarted    bool
}

// NewBroker 创建一个新的 RabbitMQ Broker 实例
func NewBroker(config Config) (broker.Broker, error) {
	return NewBrokerWithLogger(config, logger.NewProductionLogger())
}

// NewBrokerWithLogger 创建一个带有指定logger的 RabbitMQ Broker 实例
func NewBrokerWithLogger(config Config, log *logger.Logger) (broker.Broker, error) {
	if config.Namespace == "" {
		return nil, errors.New("rabbitmq config requires a non-empty namespace")
	}
	if log == nil {
		log = logger.NewProductionLogger()
	}

	if config.Prefetch <= 0 {
		config.Prefetch = 1
	}

	poolSize := config.PublishPoolSize
	if poolSize <= 0 {
		poolSize = defaultPublishPoolSize
	}
	config.PublishPoolSize = poolSize

	b := &rabbitBroker{
		config:              config,
		logger:              log,
		delayedExchangeName: fmt.Sprintf("%s.deltask.delayed", config.Namespace),
		// 初始化 Channel 池
		publishPool: make(chan *amqp.Channel, poolSize),
	}

	if err := b.connect(); err != nil {
		return nil, fmt.Errorf("failed to connect on initial setup: %w", err)
	}

	return b, nil
}

// connect 建立连接、声明交换机并预填充 Channel 池
func (b *rabbitBroker) connect() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.isClosed {
		return ErrBrokerClosed
	}

	conn, err := amqp.Dial(b.config.URL)
	if err != nil {
		return err
	}
	b.conn = conn

	// 使用临时 Channel 声明基础拓扑 (Exchange)
	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return err
	}
	defer ch.Close()

	if err := b.declareExchange(ch); err != nil {
		_ = conn.Close()
		return err
	}

	// 预填充 Publish Channel 池
	// 在持有锁的情况下进行，确保连接建立后池子立即可用
	b.fillPublishPoolLocked(conn)

	// 启动后台重连监听器
	if !b.reconnectStarted {
		b.reconnectStarted = true
		go b.handleReconnect(conn)
	} else {
		go b.handleReconnect(conn)
	}

	b.logger.Info("RabbitMQ broker connected",
		zap.String("namespace", b.config.Namespace),
		zap.String("exchange", b.delayedExchangeName),
		zap.Int("publish_pool_size", b.config.PublishPoolSize))
	return nil
}

// declareExchange 声明延迟交换机 (幂等操作)
func (b *rabbitBroker) declareExchange(ch *amqp.Channel) error {
	args := amqp.Table{"x-delayed-type": "direct"}
	if err := ch.ExchangeDeclare(
		b.delayedExchangeName, // name
		delayedExchangeType,   // type
		true,                  // durable
		false,                 // auto-deleted
		false,                 // internal
		false,                 // no-wait
		args,                  // arguments
	); err != nil {
		return fmt.Errorf("failed to declare delayed exchange: %w", err)
	}
	return nil
}

// fillPublishPoolLocked 预填充 Channel 池 (必须在锁内调用)
func (b *rabbitBroker) fillPublishPoolLocked(conn *amqp.Connection) {
	// 为了安全，使用非阻塞写入
	for range b.config.PublishPoolSize {
		ch, err := conn.Channel()
		if err != nil {
			b.logger.Error("Failed to pre-fill publish channel", zap.Error(err))
			break // 创建失败，停止填充，运行时动态创建补齐
		}

		// 尝试放入池中
		select {
		case b.publishPool <- ch:
		default:
			_ = ch.Close() // 池满则关闭
		}
	}
}

// handleReconnect 监听特定连接的关闭事件
func (b *rabbitBroker) handleReconnect(conn *amqp.Connection) {
	closeChan := conn.NotifyClose(make(chan *amqp.Error, 1))

	err := <-closeChan
	if err == nil {
		return // 主动关闭
	}

	b.logger.Warn("RabbitMQ connection lost, attempting to reconnect", zap.Error(err))

	b.mu.Lock()
	if b.conn == conn {
		b.conn = nil
	}
	b.mu.Unlock()

	// 连接断开，清空旧的 Channel 池
	b.clearPublishPool()

	const maxDelay = 30 * time.Second
	delay := time.Second

	for {
		b.mu.RLock()
		if b.isClosed {
			b.mu.RUnlock()
			return
		}
		b.mu.RUnlock()

		if err := b.connect(); err == nil {
			b.logger.Info("RabbitMQ reconnected successfully")
			return
		}

		b.logger.Info("RabbitMQ reconnection failed, retrying...", zap.Duration("retry_delay", delay))
		time.Sleep(delay)

		delay *= 2
		if delay > maxDelay {
			delay = maxDelay
		}
	}
}

// getPublishChannel 从池中获取 Channel
// 阻塞等待 + Context 超时，防止无限创建 Channel
func (b *rabbitBroker) getPublishChannel(ctx context.Context) (*amqp.Channel, error) {
	select {
	case ch := <-b.publishPool:
		// 检查取出的 channel 是否已关闭
		if ch.IsClosed() {
			// 如果池里的 Channel 坏了，尝试立即创建一个顶替
			// 这样可以维持池子的容量，防止坏 Channel 耗尽池子
			newCh, err := b.createChannel()
			if err != nil {
				// 返回错误，池子容量暂时 -1，等待 Reconnect 逻辑重置整个池子
				return nil, err
			}
			return newCh, nil
		}
		return ch, nil

	case <-ctx.Done():
		// 等待 Channel 超时（说明并发太高或连接断开池子空了）
		return nil, ctx.Err()
	}
}

// putPublishChannel 将 Channel 放回池中
func (b *rabbitBroker) putPublishChannel(ch *amqp.Channel) {
	if ch.IsClosed() {
		return
	}
	select {
	case b.publishPool <- ch:
		// 归还成功
	default:
		// 池子满了（理论上不应该发生，除非有额外的创建逻辑），关闭该 Channel
		_ = ch.Close()
	}
}

// createChannel 基于当前连接创建新 Channel (辅助方法)
func (b *rabbitBroker) createChannel() (*amqp.Channel, error) {
	b.mu.RLock()
	conn := b.conn
	closed := b.isClosed
	b.mu.RUnlock()

	if closed || conn == nil || conn.IsClosed() {
		return nil, ErrBrokerNotConnected
	}

	return conn.Channel()
}

// clearPublishPool 清空池中的所有 Channel
func (b *rabbitBroker) clearPublishPool() {
	for {
		select {
		case ch := <-b.publishPool:
			_ = ch.Close()
		default:
			return
		}
	}
}

// Publish 实现了 Broker 接口
func (b *rabbitBroker) Publish(ctx context.Context, t *task.Task, queueName string) error {
	body, err := json.Marshal(t)
	if err != nil {
		return fmt.Errorf("failed to marshal task: %w", err)
	}

	headers := amqp.Table{}
	if delay := time.Until(t.ExecuteAt).Milliseconds(); delay > 0 {
		headers["x-delay"] = delay
	}
	routingKey := b.prefixed(queueName)

	// 强制超时控制
	// 防止在高并发下因为等待 Channel 而导致的无限期挂起
	pubCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	// 阻塞获取 Channel
	ch, err := b.getPublishChannel(pubCtx)
	if err != nil {
		return fmt.Errorf("failed to get publish channel: %w", err)
	}

	// 执行发布
	err = ch.PublishWithContext(pubCtx,
		b.delayedExchangeName,
		routingKey,
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			Body:         body,
			Headers:      headers,
		})

	if err != nil {
		// 发生错误，Channel 可能已废弃，直接关闭，不放回池子
		_ = ch.Close()
		return fmt.Errorf("amqp publish error: %w", err)
	}

	// 归还 Channel
	b.putPublishChannel(ch)
	return nil
}

// Consume 实现了 Broker 接口
func (b *rabbitBroker) Consume(ctx context.Context, queueName string, opts ...broker.ConsumeOption) (<-chan *task.Task, error) {
	outChan := make(chan *task.Task)

	options := &broker.ConsumeOptions{
		PrefetchCount: b.config.Prefetch,
	}
	for _, opt := range opts {
		opt(options)
	}
	prefixedQ := b.prefixed(queueName)

	go func() {
		defer close(outChan)

		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			if err := b.waitForConnection(ctx); err != nil {
				return
			}

			// 消费者使用独占的 Channel，不通过池子获取
			ch, err := b.createChannel()
			if err != nil {
				time.Sleep(time.Second)
				continue
			}

			// 重建完整拓扑结构
			// 确保 Exchange 存在，Queue 存在，且 Bind 关系正确
			// 解决 Broker 重启后 Exchange 丢失导致 Publish 404 的问题
			if err := b.setupTopology(ch, prefixedQ, options.PrefetchCount); err != nil {
				b.logger.Error("Failed to setup topology, retrying...", zap.Error(err))
				_ = ch.Close()
				time.Sleep(3 * time.Second)
				continue
			}

			deliveries, err := ch.Consume(prefixedQ, "", false, false, false, false, nil)
			if err != nil {
				b.logger.Error("Failed to start consuming, retrying...", zap.Error(err))
				_ = ch.Close()
				time.Sleep(3 * time.Second)
				continue
			}

			b.logger.Info("Consumer loop started", zap.String("queue", prefixedQ))
			b.consumeLoop(ctx, deliveries, outChan)

			_ = ch.Close()
			b.logger.Warn("Consumer loop stopped, attempting recovery...", zap.String("queue", prefixedQ))
			time.Sleep(time.Second)
		}
	}()

	return outChan, nil
}

func (b *rabbitBroker) consumeLoop(ctx context.Context, deliveries <-chan amqp.Delivery, outChan chan<- *task.Task) {
	for {
		select {
		case <-ctx.Done():
			return
		case d, ok := <-deliveries:
			if !ok {
				return
			}

			var t task.Task
			if err := json.Unmarshal(d.Body, &t); err != nil {
				b.logger.Error("Failed to unmarshal message, dropping", zap.Error(err))
				_ = d.Nack(false, false)
				continue
			}

			t.BrokerMeta = d

			select {
			case outChan <- &t:
			case <-ctx.Done():
				return
			}
		}
	}
}

// setupTopology 负责声明 Exchange、队列、绑定和设置 QoS
func (b *rabbitBroker) setupTopology(ch *amqp.Channel, queueName string, prefetch int) error {
	// 确保 Exchange 存在，因为 Broker 可能刚重启，所有非持久化的状态都丢了
	if err := b.declareExchange(ch); err != nil {
		return err
	}

	_, err := ch.QueueDeclare(queueName, true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("queue declare: %w", err)
	}

	err = ch.QueueBind(queueName, queueName, b.delayedExchangeName, false, nil)
	if err != nil {
		return fmt.Errorf("queue bind: %w", err)
	}

	if err := ch.Qos(prefetch, 0, false); err != nil {
		return fmt.Errorf("qos: %w", err)
	}

	return nil
}

func (b *rabbitBroker) waitForConnection(ctx context.Context) error {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		b.mu.RLock()
		conn := b.conn
		closed := b.isClosed
		b.mu.RUnlock()

		if closed {
			return ErrBrokerClosed
		}
		if conn != nil && !conn.IsClosed() {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			continue
		}
	}
}

func (b *rabbitBroker) Ack(ctx context.Context, t *task.Task) error {
	delivery, ok := t.BrokerMeta.(amqp.Delivery)
	if !ok {
		return errors.New("invalid broker metadata for ack")
	}
	if err := delivery.Ack(false); err != nil {
		b.logger.Warn("Failed to Ack message", zap.Error(err))
		return err
	}
	return nil
}

func (b *rabbitBroker) Nack(ctx context.Context, t *task.Task, requeue bool) error {
	delivery, ok := t.BrokerMeta.(amqp.Delivery)
	if !ok {
		return errors.New("invalid broker metadata for nack")
	}
	if err := delivery.Nack(false, requeue); err != nil {
		b.logger.Warn("Failed to Nack message", zap.Error(err))
		return err
	}
	return nil
}

func (b *rabbitBroker) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.isClosed {
		return nil
	}
	b.isClosed = true
	b.clearPublishPool()

	if b.conn != nil {
		if err := b.conn.Close(); err != nil {
			return fmt.Errorf("close error: %w", err)
		}
	}
	b.logger.Info("RabbitMQ broker closed gracefully")
	return nil
}

func (b *rabbitBroker) prefixed(name string) string {
	return fmt.Sprintf("%s.%s", b.config.Namespace, name)
}
