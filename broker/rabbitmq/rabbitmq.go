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
)

// ErrBrokerNotConnected 表示 broker 未连接（正在重连中）
var ErrBrokerNotConnected = errors.New("broker is not connected")

// ErrBrokerReconnecting 表示 broker 正在重连中
var ErrBrokerReconnecting = errors.New("broker is reconnecting")

// ErrBrokerClosed 表示 broker 已关闭
var ErrBrokerClosed = errors.New("broker is closed")

// isChannelError 判断错误是否是通道/连接相关的错误
func isChannelError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return strings.Contains(errStr, "channel/connection is not open") ||
		strings.Contains(errStr, "channel error") ||
		strings.Contains(errStr, "connection closed") ||
		strings.Contains(errStr, "connection reset") ||
		strings.Contains(errStr, "socket closed")
}

// Config 封装了连接 RabbitMQ 所需的配置
type Config struct {
	URL       string // AMQP 连接 URL, e.g., "amqp://guest:guest@localhost:5672/my_vhost"
	Namespace string // 用于隔离资源的命名空间，如 "billing" 或 "notifications"
	// Prefetch 是消费端 QoS 预取数量。用于限制单个消费者在未确认前最多持有的消息数。
	// 设置更大的值有助于提高吞吐。若小于等于 0，则使用默认值 1。
	// 设置该值作为默认值，可通过 ConsumeOption 覆盖。
	Prefetch int
}

// rabbitBroker 实现了 broker.Broker 接口
type rabbitBroker struct {
	config Config
	logger *logger.Logger
	mu     sync.Mutex

	conn    *amqp.Connection
	channel *amqp.Channel

	delayedExchangeName string
	isClosed            bool
	reconnectStarted    bool // 标记是否已启动重连监听
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

// connect 负责建立连接和通道，并设置重连逻辑
func (b *rabbitBroker) connect() error {
	b.mu.Lock()
	defer b.mu.Unlock()

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

	// 声明延迟交换机，这是实现延迟任务的关键。
	// 它是幂等的，如果已存在则无操作。
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

	// 监听连接关闭事件，用于自动重连（只启动一次）
	b.mu.Lock()
	if !b.reconnectStarted {
		b.reconnectStarted = true
		b.mu.Unlock()
		go b.handleReconnect()
	} else {
		b.mu.Unlock()
	}

	b.logger.Info("RabbitMQ broker connected",
		zap.String("namespace", b.config.Namespace),
		zap.String("exchange", b.delayedExchangeName))
	return nil
}

// handleReconnect 监听连接关闭信号并尝试重连
func (b *rabbitBroker) handleReconnect() {
	for {
		b.mu.Lock()
		conn := b.conn
		isClosed := b.isClosed
		b.mu.Unlock()

		if isClosed {
			return
		}

		if conn == nil {
			// 连接尚未建立，等待一段时间后重试
			time.Sleep(time.Second)
			continue
		}

		closeChan := conn.NotifyClose(make(chan *amqp.Error))

		err := <-closeChan
		// 连接关闭，检查是否是主动关闭
		b.mu.Lock()
		if b.isClosed {
			b.mu.Unlock()
			return
		}
		// 清理旧的连接和通道引用
		b.channel = nil
		b.conn = nil
		b.mu.Unlock()

		if err != nil {
			b.logger.Warn("RabbitMQ connection closed, attempting to reconnect", zap.Error(err))
		}

		// 使用指数退避策略进行重连
		const maxReconnectDelay = 30 * time.Second
		reconnectDelay := time.Second

		for {
			// 等待后重试
			time.Sleep(reconnectDelay)

			// 检查是否已关闭（Sleep 后检查一次即可）
			b.mu.Lock()
			if b.isClosed {
				b.mu.Unlock()
				return
			}
			b.mu.Unlock()

			b.logger.Info("Attempting to reconnect to RabbitMQ",
				zap.Duration("retry_delay", reconnectDelay))

			if err := b.connect(); err == nil {
				b.logger.Info("RabbitMQ reconnected successfully")
				// 重连成功，继续监听新的连接
				break
			}
			b.logger.Error("RabbitMQ reconnection failed", zap.Error(err))

			// 指数退避
			reconnectDelay *= 2
			if reconnectDelay > maxReconnectDelay {
				reconnectDelay = maxReconnectDelay
			}
		}
	}
}

// prefixed 为给定的名称加上命名空间前缀
func (b *rabbitBroker) prefixed(name string) string {
	return fmt.Sprintf("%s.%s", b.config.Namespace, name)
}

// Publish 实现了 Broker 接口的 Publish 方法
func (b *rabbitBroker) Publish(ctx context.Context, t *task.Task, queueName string) error {
	const maxPublishRetries = 5
	retryCount := 0

	for {
		b.mu.Lock()
		channel := b.channel
		conn := b.conn
		isClosed := b.isClosed
		b.mu.Unlock()

		if isClosed {
			return errors.New("broker is closed")
		}

		if channel == nil {
			// 如果从未建立过连接（conn 也是 nil），立即返回错误
			if conn == nil {
				return ErrBrokerNotConnected
			}
			// 通道正在重连，等待后重试
			if retryCount >= maxPublishRetries {
				return errors.New("broker is reconnecting, max retries exceeded")
			}
			retryCount++
			delay := time.Second << min(retryCount, 3)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
				continue
			}
		}

		body, err := json.Marshal(t)
		if err != nil {
			return fmt.Errorf("failed to marshal task: %w", err)
		}

		headers := amqp.Table{}
		// 如果任务的期望执行时间在未来，计算延迟毫秒数
		if delay := time.Until(t.ExecuteAt).Milliseconds(); delay > 0 {
			headers["x-delay"] = delay
		}

		routingKey := b.prefixed(queueName)

		err = channel.PublishWithContext(ctx,
			b.delayedExchangeName, // exchange
			routingKey,            // routing key
			false,                 // mandatory
			false,                 // immediate
			amqp.Publishing{
				ContentType:  "application/json",
				DeliveryMode: amqp.Persistent, // 保证消息持久化
				Body:         body,
				Headers:      headers,
			})

		if err != nil {
			// 发布失败，检查是否是连接问题
			b.mu.Lock()
			if b.channel == nil || b.conn == nil || b.conn.IsClosed() {
				b.mu.Unlock()
				retryCount++
				if retryCount >= maxPublishRetries {
					return errors.New("broker connection lost, max retries exceeded")
				}
				// 通道已失效，循环重试
				continue
			}
			b.mu.Unlock()
			return fmt.Errorf("failed to publish task: %w", err)
		}

		return nil
	}
}

// Consume 实现了 Broker 接口的 Consume 方法
func (b *rabbitBroker) Consume(ctx context.Context, queueName string, opts ...broker.ConsumeOption) (<-chan *task.Task, error) {
	b.mu.Lock()
	channel := b.channel
	conn := b.conn
	isClosed := b.isClosed
	b.mu.Unlock()

	if isClosed {
		return nil, ErrBrokerClosed
	}

	if channel == nil || conn == nil || conn.IsClosed() {
		return nil, ErrBrokerReconnecting
	}

	consumeOptions := &broker.ConsumeOptions{
		PrefetchCount: b.config.Prefetch,
	}
	for _, opt := range opts {
		opt(consumeOptions)
	}

	prefixedQueueName := b.prefixed(queueName)

	// 声明队列 (幂等)
	_, err := channel.QueueDeclare(prefixedQueueName, true, false, false, false, nil)
	if err != nil {
		// 如果是连接/通道错误，返回 broker 未连接
		if isChannelError(err) {
			return nil, ErrBrokerNotConnected
		}
		return nil, fmt.Errorf("failed to declare queue '%s': %w", prefixedQueueName, err)
	}

	// 将队列绑定到延迟交换机 (幂等)
	err = channel.QueueBind(prefixedQueueName, prefixedQueueName, b.delayedExchangeName, false, nil)
	if err != nil {
		if isChannelError(err) {
			return nil, ErrBrokerNotConnected
		}
		return nil, fmt.Errorf("failed to bind queue '%s': %w", prefixedQueueName, err)
	}

	// 设置 QoS 预取数量（prefetch）
	if err := channel.Qos(consumeOptions.PrefetchCount, 0, false); err != nil {
		if isChannelError(err) {
			return nil, ErrBrokerNotConnected
		}
		return nil, fmt.Errorf("failed to set QoS (prefetch=%d): %w", consumeOptions.PrefetchCount, err)
	}

	deliveries, err := channel.Consume(prefixedQueueName, "", false, false, false, false, nil)
	if err != nil {
		if isChannelError(err) {
			return nil, ErrBrokerNotConnected
		}
		return nil, fmt.Errorf("failed to start consuming from queue '%s': %w", prefixedQueueName, err)
	}

	msgChan := make(chan *task.Task)

	go func() {
		defer close(msgChan)
		defer func() {
			if r := recover(); r != nil {
				b.logger.Error("Consume goroutine panic", zap.Any("panic", r))
			}
		}()
		for {
			select {
			case <-ctx.Done():
				b.logger.Info("Stop consuming from queue", zap.String("queue", prefixedQueueName))
				return
			case d, ok := <-deliveries:
				if !ok {
					b.logger.Warn("AMQP delivery channel closed for queue", zap.String("queue", prefixedQueueName))
					return
				}
				var t task.Task
				if err := json.Unmarshal(d.Body, &t); err != nil {
					b.logger.Error("Failed to unmarshal message body (dropping)", zap.Error(err))
					// 使用 requeue=false 丢弃格式错误的消息，忽略 Nack 错误
					_ = d.Nack(false, false)
					continue
				}
				// 将 amqp.Delivery 存入 BrokerMeta，以便 Ack/Nack 时使用
				t.BrokerMeta = d
				msgChan <- &t
			}
		}
	}()

	return msgChan, nil
}

// Ack 确认消息
func (b *rabbitBroker) Ack(ctx context.Context, t *task.Task) error {
	delivery, ok := t.BrokerMeta.(amqp.Delivery)
	if !ok {
		return errors.New("invalid broker metadata for ack: not an amqp.Delivery")
	}

	// 忽略连接错误：如果连接已断开，消息会被 RabbitMQ 重新投递
	// 这样可以避免 Ack/Nack 时的竞态条件
	_ = delivery.Ack(false)
	return nil
}

// Nack 拒绝消息
func (b *rabbitBroker) Nack(ctx context.Context, t *task.Task, requeue bool) error {
	delivery, ok := t.BrokerMeta.(amqp.Delivery)
	if !ok {
		return errors.New("invalid broker metadata for nack: not an amqp.Delivery")
	}

	// 忽略连接错误：如果连接已断开，消息会被 RabbitMQ 重新投递
	_ = delivery.Nack(false, requeue)
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
		return fmt.Errorf("errors while closing broker: %v", errs)
	}
	b.logger.Info("RabbitMQ broker closed gracefully")
	return nil
}
