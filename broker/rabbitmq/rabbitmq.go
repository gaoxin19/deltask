// Package rabbitmq 提供了 Deltask 的 RabbitMQ Broker 实现
// 利用 rabbitmq-delayed-message-exchange 插件来支持延迟任务
package rabbitmq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/gaoxin19/deltask/broker"
	"github.com/gaoxin19/deltask/logger"
	"github.com/gaoxin19/deltask/task"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
)

const (
	delayedExchangeType = "x-delayed-message"
	maxPublishRetries   = 3
)

// 定义常见错误
var (
	ErrBrokerNotConnected = errors.New("broker is not connected")
	ErrBrokerClosed       = errors.New("broker is closed")
)

// Config 封装了连接 RabbitMQ 所需的配置
type Config struct {
	URL       string // AMQP 连接 URL
	Namespace string // 用于隔离资源的命名空间
	// Prefetch 是消费端 QoS 预取数量。
	Prefetch int
}

// rabbitBroker 实现了 broker.Broker 接口
type rabbitBroker struct {
	config Config
	logger *logger.Logger
	mu     sync.RWMutex // 使用读写锁

	conn    *amqp.Connection
	channel *amqp.Channel

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

	b := &rabbitBroker{
		config:              config,
		logger:              log,
		delayedExchangeName: fmt.Sprintf("%s.deltask.delayed", config.Namespace),
	}

	if err := b.connect(); err != nil {
		return nil, fmt.Errorf("failed to connect on initial setup: %w", err)
	}

	return b, nil
}

// connect 建立连接、通道并声明交换机
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

	channel, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return err
	}
	b.channel = channel

	// 声明延迟交换机 (幂等)
	args := amqp.Table{"x-delayed-type": "direct"}
	if err := channel.ExchangeDeclare(
		b.delayedExchangeName, // name
		delayedExchangeType,   // type
		true,                  // durable
		false,                 // auto-deleted
		false,                 // internal
		false,                 // no-wait
		args,                  // arguments
	); err != nil {
		_ = channel.Close()
		_ = conn.Close()
		return fmt.Errorf("failed to declare delayed exchange: %w", err)
	}

	// 启动后台重连监听器（仅一次）
	if !b.reconnectStarted {
		b.reconnectStarted = true
		go b.handleReconnect(conn)
	} else {
		// 如果是重连，需要为新连接启动监听
		go b.handleReconnect(conn)
	}

	b.logger.Info("RabbitMQ broker connected",
		zap.String("namespace", b.config.Namespace),
		zap.String("exchange", b.delayedExchangeName))
	return nil
}

// handleReconnect 监听特定连接的关闭事件
// 注意：每次建立新连接都会启动一个新的 handleReconnect goroutine 监听该特定连接
func (b *rabbitBroker) handleReconnect(conn *amqp.Connection) {
	closeChan := conn.NotifyClose(make(chan *amqp.Error, 1))

	err := <-closeChan
	// 如果 err 为 nil，说明是主动关闭 (Close方法调用)，不需要重连
	if err == nil {
		return
	}

	b.logger.Warn("RabbitMQ connection lost, attempting to reconnect", zap.Error(err))

	b.mu.Lock()
	// 清理旧引用，防止使用已关闭的 channel
	if b.conn == conn {
		b.channel = nil
		b.conn = nil
	}
	b.mu.Unlock()

	// 重连循环
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

// getChannel 安全地获取当前可用的 channel
func (b *rabbitBroker) getChannel() (*amqp.Channel, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if b.isClosed {
		return nil, ErrBrokerClosed
	}
	if b.channel == nil {
		return nil, ErrBrokerNotConnected
	}
	return b.channel, nil
}

// Publish 实现了 Broker 接口
func (b *rabbitBroker) Publish(ctx context.Context, t *task.Task, queueName string) error {
	retryCount := 0

	for {
		// 获取 Channel
		ch, err := b.getChannel()
		if err != nil {
			// 如果连接未就绪，且重试次数未耗尽，则等待
			if retryCount >= maxPublishRetries {
				return fmt.Errorf("publish failed after retries: %w", err)
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Second * time.Duration(retryCount+1)):
				retryCount++
				continue
			}
		}

		// 序列化
		body, err := json.Marshal(t)
		if err != nil {
			return fmt.Errorf("failed to marshal task: %w", err) // 无法恢复的错误，不重试
		}

		headers := amqp.Table{}
		if delay := time.Until(t.ExecuteAt).Milliseconds(); delay > 0 {
			headers["x-delay"] = delay
		}
		routingKey := b.prefixed(queueName)

		// 执行发布
		err = ch.PublishWithContext(ctx,
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
			// 错误处理
			if isChannelError(err) {
				// 连接问题，重试
				retryCount++
				if retryCount > maxPublishRetries {
					return fmt.Errorf("publish connection lost: %w", err)
				}
				// 给一点时间让重连协程工作
				time.Sleep(500 * time.Millisecond)
				continue
			}
			// 其他错误（如 Exchange 不存在等），直接返回
			return fmt.Errorf("amqp publish error: %w", err)
		}

		return nil
	}
}

// Consume 实现了 Broker 接口，具备自动恢复（Auto-Recovery）能力
func (b *rabbitBroker) Consume(ctx context.Context, queueName string, opts ...broker.ConsumeOption) (<-chan *task.Task, error) {
	// 创建对外的 channel
	outChan := make(chan *task.Task)

	options := &broker.ConsumeOptions{
		PrefetchCount: b.config.Prefetch,
	}
	for _, opt := range opts {
		opt(options)
	}
	prefixedQ := b.prefixed(queueName)

	// 启动守护协程
	go func() {
		defer close(outChan)

		for {
			// 检查上下文是否取消
			select {
			case <-ctx.Done():
				return
			default:
			}

			// 等待连接可用
			ch, err := b.waitForConnection(ctx)
			if err != nil {
				// ctx canceled
				return
			}

			// 初始化拓扑结构 (队列声明与绑定)
			// Broker重启后，非持久化队列/绑定关系可能会丢失，必须重建
			if err := b.setupTopology(ch, prefixedQ, options.PrefetchCount); err != nil {
				b.logger.Error("Failed to setup topology, retrying in 3s", zap.Error(err))
				time.Sleep(3 * time.Second)
				continue
			}

			// 开始消费
			deliveries, err := ch.Consume(
				prefixedQ, // queue
				"",        // consumer
				false,     // auto-ack
				false,     // exclusive
				false,     // no-local
				false,     // no-wait
				nil,       // args
			)
			if err != nil {
				b.logger.Error("Failed to start consuming, retrying in 3s", zap.Error(err))
				time.Sleep(3 * time.Second)
				continue
			}

			b.logger.Info("Consumer loop started", zap.String("queue", prefixedQ))

			// 消息处理循环
			// 如果连接断开，deliveries 通道会被关闭，循环结束，外层 for 循环将触发重连流程
			b.consumeLoop(ctx, deliveries, outChan)

			b.logger.Warn("Consumer loop stopped, attempting recovery...", zap.String("queue", prefixedQ))
			time.Sleep(time.Second) // 避免疯狂重试
		}
	}()

	return outChan, nil
}

// consumeLoop 从 amqp 通道读取消息并转发到内部通道
func (b *rabbitBroker) consumeLoop(ctx context.Context, deliveries <-chan amqp.Delivery, outChan chan<- *task.Task) {
	for {
		select {
		case <-ctx.Done():
			return
		case d, ok := <-deliveries:
			if !ok {
				// amqp channel closed
				return
			}

			var t task.Task
			if err := json.Unmarshal(d.Body, &t); err != nil {
				b.logger.Error("Failed to unmarshal message, dropping", zap.Error(err))
				// 格式错误的消息无法重试，必须丢弃 (requeue=false)
				_ = d.Nack(false, false)
				continue
			}

			// 注入元数据以便 Ack/Nack
			t.BrokerMeta = d

			// 发送给处理者，如果处理者忙，这里会阻塞，形成背压
			select {
			case outChan <- &t:
			case <-ctx.Done():
				return
			}
		}
	}
}

// setupTopology 负责声明队列、绑定交换机和设置 QoS
func (b *rabbitBroker) setupTopology(ch *amqp.Channel, queueName string, prefetch int) error {
	// 声明队列
	_, err := ch.QueueDeclare(queueName, true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("queue declare: %w", err)
	}

	// 绑定到延迟交换机
	err = ch.QueueBind(queueName, queueName, b.delayedExchangeName, false, nil)
	if err != nil {
		return fmt.Errorf("queue bind: %w", err)
	}

	// 设置 QoS
	if err := ch.Qos(prefetch, 0, false); err != nil {
		return fmt.Errorf("qos: %w", err)
	}

	return nil
}

// waitForConnection 阻塞等待直到获取有效 Channel 或上下文取消
func (b *rabbitBroker) waitForConnection(ctx context.Context) (*amqp.Channel, error) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		ch, err := b.getChannel()
		if err == nil {
			return ch, nil
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
			continue
		}
	}
}

// Ack 确认消息
func (b *rabbitBroker) Ack(ctx context.Context, t *task.Task) error {
	delivery, ok := t.BrokerMeta.(amqp.Delivery)
	if !ok {
		return errors.New("invalid broker metadata for ack")
	}

	if err := delivery.Ack(false); err != nil {
		// 记录错误，虽然如果连接断了无法补救，但需要知道发生了错误
		b.logger.Warn("Failed to Ack message", zap.Error(err))
		return err
	}
	return nil
}

// Nack 拒绝消息
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

// Close 关闭连接
func (b *rabbitBroker) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.isClosed {
		return nil
	}
	b.isClosed = true

	var errs []error
	if b.channel != nil {
		if err := b.channel.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if b.conn != nil {
		if err := b.conn.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("close errors: %v", errs)
	}
	b.logger.Info("RabbitMQ broker closed gracefully")
	return nil
}

func (b *rabbitBroker) prefixed(name string) string {
	return fmt.Sprintf("%s.%s", b.config.Namespace, name)
}

func isChannelError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return strings.Contains(errStr, "channel/connection is not open") ||
		strings.Contains(errStr, "channel error") ||
		strings.Contains(errStr, "connection closed") ||
		strings.Contains(errStr, "connection reset") ||
		strings.Contains(errStr, "socket closed") ||
		errors.Is(err, amqp.ErrClosed)
}
