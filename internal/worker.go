package internal

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"time"

	"github.com/gaoxin19/deltask/broker"
	"github.com/gaoxin19/deltask/broker/rabbitmq"
	deltacontext "github.com/gaoxin19/deltask/context"
	"github.com/gaoxin19/deltask/logger"
	"github.com/gaoxin19/deltask/task"
	"go.uber.org/zap"
)

// RetryPolicy 定义重试策略
type RetryPolicy struct {
	MaxRetries  int           // 最大重试次数
	BaseDelay   time.Duration // 基础延迟时间
	MaxDelay    time.Duration // 最大延迟时间
	Exponential bool          // 是否使用指数退避
}

// DefaultRetryPolicy 返回默认的重试策略
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		MaxRetries:  3,
		BaseDelay:   time.Second,
		MaxDelay:    time.Minute,
		Exponential: true,
	}
}

// Worker 是 Worker 的内部实现。
type Worker struct {
	broker      broker.Broker
	queueName   string
	concurrency int
	registry    map[string]task.Handler
	retryPolicy RetryPolicy
	logger      *logger.Logger
	wg          sync.WaitGroup
	cancel      context.CancelFunc
}

func NewWorker(b broker.Broker, queueName string, concurrency int) *Worker {
	return NewWorkerWithLogger(b, queueName, concurrency, logger.NewProductionLogger())
}

func NewWorkerWithLogger(b broker.Broker, queueName string, concurrency int, log *logger.Logger) *Worker {
	if concurrency <= 0 {
		concurrency = 1
	}
	if log == nil {
		log = logger.NewProductionLogger()
	}
	return &Worker{
		broker:      b,
		queueName:   queueName,
		concurrency: concurrency,
		registry:    make(map[string]task.Handler),
		retryPolicy: DefaultRetryPolicy(),
		logger:      log,
	}
}

// WithRetryPolicy 设置重试策略
func (w *Worker) WithRetryPolicy(policy RetryPolicy) *Worker {
	w.retryPolicy = policy
	return w
}

func (w *Worker) QueueName() string {
	return w.queueName
}

// Register 注册任务处理器
func (w *Worker) Register(taskName string, handler task.Handler) {
	w.registry[taskName] = handler
}

func (w *Worker) Run(ctx context.Context) error {
	// 创建可取消的上下文
	workerCtx, cancel := context.WithCancel(ctx)
	w.cancel = cancel

	brokerConsumeOptions := []broker.ConsumeOption{
		broker.WithPrefetchCount(w.concurrency), // 设置预取数量与并发数一致
	}

	w.logger.Info("Starting workers for queue",
		zap.Int("concurrency", w.concurrency),
		zap.String("queue", w.queueName))

	// 启动 worker goroutines
	for i := range w.concurrency {
		w.wg.Add(1)
		go func(workerID int) {
			defer w.wg.Done()
			w.logger.Info("Worker started", zap.Int("worker_id", workerID))
			w.runWorkerLoop(workerCtx, workerID, brokerConsumeOptions)
		}(i + 1)
	}

	w.wg.Wait()
	w.logger.Info("All workers have shut down")
	return nil
}

// runWorkerLoop 运行单个 worker 的主循环，支持自动重新消费
func (w *Worker) runWorkerLoop(ctx context.Context, workerID int, brokerConsumeOptions []broker.ConsumeOption) {
	const maxReconnectDelay = 30 * time.Second
	const baseReconnectDelay = time.Second
	const reconnectingDelay = 5 * time.Second // 检测到重连时的等待时间
	reconnectDelay := baseReconnectDelay

	// 为每个 worker 创建独立的随机数生成器，避免同步重试
	rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)))

	for {
		select {
		case <-ctx.Done():
			w.logger.Info("Worker shutting down", zap.Int("worker_id", workerID))
			return
		default:
		}

		// 尝试开始消费
		msgChan, err := w.broker.Consume(ctx, w.queueName, brokerConsumeOptions...)
		if err != nil {
			// 检测是否是正在重连的错误
			isReconnecting := errors.Is(err, rabbitmq.ErrBrokerReconnecting)

			w.logger.Error("Failed to start consuming, will retry",
				zap.Int("worker_id", workerID),
				zap.String("queue", w.queueName),
				zap.Error(err),
				zap.Bool("reconnecting", isReconnecting),
				zap.Duration("retry_delay", reconnectDelay))

			// 如果是等待重连的错误，给 handleReconnect 更多时间
			waitDelay := reconnectDelay
			if isReconnecting {
				waitDelay = reconnectingDelay
			}

			// 等待后重试（添加随机抖动避免同步重试）
			actualDelay := w.addJitter(waitDelay, rng)
			select {
			case <-ctx.Done():
				return
			case <-time.After(actualDelay):
				if !isReconnecting {
					// 指数退避，但不超过最大值
					reconnectDelay *= 2
					if reconnectDelay > maxReconnectDelay {
						reconnectDelay = maxReconnectDelay
					}
				}
				continue
			}
		}

		// 成功连接，重置重连延迟
		reconnectDelay = baseReconnectDelay
		w.logger.Info("Worker started consuming",
			zap.Int("worker_id", workerID),
			zap.String("queue", w.queueName))

		// 处理消息直到通道关闭
		channelClosed := false
		for {
			select {
			case <-ctx.Done():
				w.logger.Info("Worker shutting down", zap.Int("worker_id", workerID))
				return
			case t, ok := <-msgChan:
				if !ok {
					// 消息通道关闭，可能是连接断开
					w.logger.Warn("Message channel closed, will attempt to reconnect",
						zap.Int("worker_id", workerID),
						zap.String("queue", w.queueName))
					channelClosed = true
					break
				}
				// 使用worker上下文，允许任务取消
				w.processTask(ctx, t)
			}
			if channelClosed {
				break
			}
		}

		// 通道关闭，添加初始延迟避免立即重试（减少惊群效应）
		// 使用较小的初始延迟 + 随机抖动，让不同 worker 错开重试时间
		initialDelay := baseReconnectDelay / 2
		actualDelay := w.addJitter(initialDelay, rng)
		select {
		case <-ctx.Done():
			return
		case <-time.After(actualDelay):
			// 指数退避
			reconnectDelay *= 2
			if reconnectDelay > maxReconnectDelay {
				reconnectDelay = maxReconnectDelay
			}
		}
	}
}

// addJitter 添加随机抖动到延迟时间，避免多个 worker 同步重试
// 抖动范围：±25%，例如 1s -> 0.75s-1.25s
func (w *Worker) addJitter(delay time.Duration, rng *rand.Rand) time.Duration {
	if delay <= 0 {
		return delay
	}
	// 添加 ±25% 的随机抖动
	jitter := time.Duration(float64(delay) * 0.25 * (2*rng.Float64() - 1))
	result := delay + jitter
	if result < 0 {
		return delay
	}
	return result
}

// Stop 优雅地停止worker
func (w *Worker) Stop() {
	if w.cancel != nil {
		w.logger.Info("Stopping worker...")
		w.cancel()
		w.wg.Wait()
		w.logger.Info("Worker stopped gracefully")
	}
}

func (w *Worker) processTask(ctx context.Context, t *task.Task) {
	// 添加panic恢复机制
	defer func() {
		if r := recover(); r != nil {
			w.logger.Error("Task handler panicked",
				zap.String("task_name", t.Name),
				zap.String("task_id", t.ID),
				zap.Any("panic_value", r),
				zap.String("stack_trace", getStackTrace()))

			// 将panic视为任务执行失败，走正常的失败处理流程
			w.handleTaskFailure(ctx, t, fmt.Errorf("task handler panicked: %v", r))
		}
	}()

	handler, ok := w.registry[t.Name]
	if !ok {
		w.handleUnknownTask(ctx, t)
		return
	}

	w.logTaskStart(t)

	// 创建增强的 Context
	taskInfo := &deltacontext.TaskInfo{
		ID:    t.ID,
		Name:  t.Name,
		Retry: t.Retry,
	}
	deltaCtx := deltacontext.New(ctx, t.Payload, taskInfo)

	_, err := handler(deltaCtx)
	if err != nil {
		w.logTaskFailure(t, err)
		w.handleTaskFailure(ctx, t, err)
	} else {
		w.handleTaskSuccess(ctx, t)
	}
}

// handleUnknownTask 处理未注册的任务
func (w *Worker) handleUnknownTask(ctx context.Context, t *task.Task) {
	w.logger.Error("No handler registered for task",
		zap.String("task_name", t.Name),
		zap.String("task_id", t.ID))
	if err := w.broker.Nack(ctx, t, false); err != nil { // 不重入队列，直接丢弃
		w.logger.Error("Failed to Nack unknown task",
			zap.String("task_id", t.ID),
			zap.Error(err))
	}
}

// handleTaskSuccess 处理任务成功完成
func (w *Worker) handleTaskSuccess(ctx context.Context, t *task.Task) {
	w.logger.Info("Task completed",
		zap.String("task_name", t.Name),
		zap.String("task_id", t.ID))
	if err := w.broker.Ack(ctx, t); err != nil {
		// 如果是连接错误，记录警告但不影响任务处理（任务已经成功执行）
		// 消息会在连接恢复后由 RabbitMQ 重新投递
		w.logger.Warn("Failed to Ack successful task (connection may be lost, message will be redelivered)",
			zap.String("task_id", t.ID),
			zap.Error(err))
	}
}

// logTaskStart 记录任务开始处理的日志
func (w *Worker) logTaskStart(t *task.Task) {
	w.logger.Info("Processing task",
		zap.String("task_name", t.Name),
		zap.String("task_id", t.ID),
		zap.Int("retry", t.Retry),
		zap.Int("max_retries", w.retryPolicy.MaxRetries))
}

// logTaskFailure 记录任务失败的日志
func (w *Worker) logTaskFailure(t *task.Task, err error) {
	w.logger.Error("Task failed",
		zap.String("task_name", t.Name),
		zap.String("task_id", t.ID),
		zap.Error(err))
}

// handleTaskFailure 处理任务失败，实现重试逻辑
func (w *Worker) handleTaskFailure(ctx context.Context, t *task.Task, err error) {
	// 检查是否超过最大重试次数
	if w.shouldMoveToDeadLetter(t) {
		w.handleDeadLetterTask(ctx, t)
		return
	}

	// 尝试重试任务
	w.retryTask(ctx, t)
}

// shouldMoveToDeadLetter 判断任务是否应该移到死信队列
func (w *Worker) shouldMoveToDeadLetter(t *task.Task) bool {
	return t.Retry >= w.retryPolicy.MaxRetries
}

// handleDeadLetterTask 处理超过最大重试次数的任务
func (w *Worker) handleDeadLetterTask(ctx context.Context, t *task.Task) {
	w.logger.Error("Task exceeded max retries, moving to dead letter",
		zap.String("task_name", t.Name),
		zap.String("task_id", t.ID),
		zap.Int("max_retries", w.retryPolicy.MaxRetries))

	// 超过最大重试次数，不重新入队
	if err := w.broker.Nack(ctx, t, false); err != nil {
		w.logger.Error("Failed to Nack dead task",
			zap.String("task_id", t.ID),
			zap.Error(err))
	}
}

// retryTask 重试任务
func (w *Worker) retryTask(ctx context.Context, t *task.Task) {
	// 计算重试延迟
	delay := w.calculateRetryDelay(t.Retry)

	// 创建重试任务
	retryTask := w.createRetryTask(t, delay)

	w.logRetryAttempt(t, delay, retryTask.Retry)

	// 将重试任务发布到同一队列
	if err := w.broker.Publish(ctx, retryTask, w.queueName); err != nil {
		w.handleRetryPublishFailure(ctx, t, err)
		return
	}

	// 确认原任务（因为我们已经重新发布了）
	if err := w.broker.Ack(ctx, t); err != nil {
		// 如果是连接错误，记录警告（重试任务已经发布，原任务会在连接恢复后重新投递）
		w.logger.Warn("Failed to Ack retried task (connection may be lost, message will be redelivered)",
			zap.String("task_id", t.ID),
			zap.Error(err))
	}
}

// createRetryTask 创建重试任务
func (w *Worker) createRetryTask(t *task.Task, delay time.Duration) *task.Task {
	return &task.Task{
		ID:        t.ID,
		Name:      t.Name,
		Payload:   t.Payload,
		Retry:     t.Retry + 1,
		ExecuteAt: time.Now().Add(delay),
	}
}

// logRetryAttempt 记录重试尝试的日志
func (w *Worker) logRetryAttempt(t *task.Task, delay time.Duration, retryCount int) {
	w.logger.Info("Scheduling retry for task",
		zap.String("task_name", t.Name),
		zap.String("task_id", t.ID),
		zap.Duration("delay", delay),
		zap.Int("attempt", retryCount),
		zap.Int("max_retries", w.retryPolicy.MaxRetries))
}

// handleRetryPublishFailure 处理重试发布失败的情况
func (w *Worker) handleRetryPublishFailure(ctx context.Context, t *task.Task, err error) {
	w.logger.Error("Failed to publish retry task",
		zap.String("task_id", t.ID),
		zap.Error(err))
	// 发布失败，拒绝原任务且不重新入队
	if nackErr := w.broker.Nack(ctx, t, false); nackErr != nil {
		w.logger.Error("Failed to Nack failed retry task",
			zap.String("task_id", t.ID),
			zap.Error(nackErr))
	}
}

// calculateRetryDelay 计算重试延迟时间
func (w *Worker) calculateRetryDelay(retryCount int) time.Duration {
	if !w.retryPolicy.Exponential {
		return w.retryPolicy.BaseDelay
	}

	// 指数退避：baseDelay * 2^retryCount
	delay := w.retryPolicy.BaseDelay
	for range retryCount {
		delay *= 2
		if delay > w.retryPolicy.MaxDelay {
			return w.retryPolicy.MaxDelay
		}
	}

	return delay
}

// getStackTrace 获取当前goroutine的堆栈信息
func getStackTrace() string {
	const maxStackFrames = 32
	var stack [maxStackFrames]uintptr

	// 获取调用栈
	n := runtime.Callers(3, stack[:]) // 跳过前3层调用（runtime.Callers, getStackTrace, defer函数）

	if n == 0 {
		return "no stack trace available"
	}

	// 构建堆栈字符串
	var result string
	frames := runtime.CallersFrames(stack[:n])

	for {
		frame, more := frames.Next()
		result += fmt.Sprintf("\n\t%s:%d", frame.Function, frame.Line)
		if !more {
			break
		}
	}

	return result
}
