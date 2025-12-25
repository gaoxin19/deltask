package internal

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/gaoxin19/deltask/broker"
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

func (w *Worker) Register(taskName string, handler task.Handler) {
	w.registry[taskName] = handler
}

// QueueName 返回 Worker 监听的队列名称
func (w *Worker) QueueName() string {
	return w.queueName
}

// Run 启动 Worker，该方法会阻塞直到 ctx 被取消
func (w *Worker) Run(ctx context.Context) error {
	w.logger.Info("Starting workers",
		zap.Int("concurrency", w.concurrency),
		zap.String("queue", w.queueName))

	// 获取消息通道
	// 注意：Broker 实现必须支持自动重连，返回一个长期有效的 Channel
	msgChan, err := w.broker.Consume(ctx, w.queueName, broker.WithPrefetchCount(w.concurrency))
	if err != nil {
		return fmt.Errorf("failed to start consumer: %w", err)
	}

	for i := 0; i < w.concurrency; i++ {
		w.wg.Add(1)
		go func(id int) {
			defer w.wg.Done()
			w.runWorker(ctx, id, msgChan)
		}(i + 1)
	}

	// 等待所有 Worker 退出（通常由 ctx.Done 触发）
	w.wg.Wait()
	w.logger.Info("All workers stopped")
	return nil
}

// runWorker 极简的 Worker 循环
func (w *Worker) runWorker(ctx context.Context, id int, msgChan <-chan *task.Task) {
	w.logger.Info("Worker started", zap.Int("worker_id", id))

	for {
		select {
		case <-ctx.Done():
			return
		case t, ok := <-msgChan:
			if !ok {
				// Broker 关闭了通道，说明发生了不可恢复的错误或显式关闭
				w.logger.Warn("Message channel closed", zap.Int("worker_id", id))
				return
			}
			w.processTask(ctx, t)
		}
	}
}

func (w *Worker) processTask(ctx context.Context, t *task.Task) {
	// Panic 保护
	defer func() {
		if r := recover(); r != nil {
			stack := getStackTrace()
			w.logger.Error("Task handler panicked",
				zap.String("task_id", t.ID),
				zap.Any("panic", r),
				zap.String("stack", stack))

			// Panic 视为执行失败
			w.handleTaskFailure(ctx, t, fmt.Errorf("panic: %v", r))
		}
	}()

	handler, ok := w.registry[t.Name]
	if !ok {
		w.logger.Error("Unknown task type", zap.String("name", t.Name))
		// 未知任务直接丢弃，不重试
		_ = w.broker.Nack(ctx, t, false)
		return
	}

	w.logger.Debug("Processing task", zap.String("id", t.ID), zap.String("name", t.Name))

	// 构造上下文
	taskInfo := &deltacontext.TaskInfo{
		ID:    t.ID,
		Name:  t.Name,
		Retry: t.Retry,
	}
	deltaCtx := deltacontext.New(ctx, t.Payload, taskInfo)

	// 执行业务逻辑
	_, err := handler(deltaCtx)
	if err != nil {
		w.logger.Error("Task execution failed", zap.String("id", t.ID), zap.Error(err))
		w.handleTaskFailure(ctx, t, err)
	} else {
		w.logger.Info("Task completed", zap.String("id", t.ID))
		if ackErr := w.broker.Ack(ctx, t); ackErr != nil {
			// Ack 失败仅记录日志，由 Broker 重发机制保证至少一次执行
			w.logger.Warn("Ack failed", zap.String("id", t.ID), zap.Error(ackErr))
		}
	}
}

func (w *Worker) handleTaskFailure(ctx context.Context, t *task.Task, err error) {
	// 检查是否达到最大重试次数
	if t.Retry >= w.retryPolicy.MaxRetries {
		w.logger.Error("Max retries reached, dropping task",
			zap.String("id", t.ID),
			zap.Int("retries", t.Retry))
		// 死信处理：直接丢弃
		_ = w.broker.Nack(ctx, t, false)
		return
	}

	// 计算延迟并发布重试任务
	delay := w.calculateRetryDelay(t.Retry)
	retryTask := &task.Task{
		ID:        t.ID, // 保持 ID 不变，方便追踪
		Name:      t.Name,
		Payload:   t.Payload,
		Retry:     t.Retry + 1,
		ExecuteAt: time.Now().Add(delay),
	}

	w.logger.Info("Scheduling retry",
		zap.String("id", t.ID),
		zap.Int("next_retry", retryTask.Retry),
		zap.Duration("delay", delay))

	// 先发布，后 Ack
	if pubErr := w.broker.Publish(ctx, retryTask, w.queueName); pubErr != nil {
		w.logger.Error("Failed to publish retry task", zap.Error(pubErr))

		// 发布重试任务失败，不能丢弃原任务（Nack false）。
		//  Nack 并 Requeue (true)，让当前任务立即回到队列头部，
		// 这样它会被再次消费，再次进入这个流程，直到 Publish 成功。
		_ = w.broker.Nack(ctx, t, true)
		return
	}

	// 发布成功后，Ack 原任务
	if ackErr := w.broker.Ack(ctx, t); ackErr != nil {
		w.logger.Warn("Ack failed after retry scheduled", zap.Error(ackErr))
		// 这里虽然可能导致双重投递（Publish成功但Ack失败），但这是权衡后的最优解。
		// 业务处理必须幂等。
	}
}

func (w *Worker) calculateRetryDelay(retryCount int) time.Duration {
	if !w.retryPolicy.Exponential {
		return w.retryPolicy.BaseDelay
	}
	delay := w.retryPolicy.BaseDelay * (1 << retryCount) // 2^retryCount
	if delay > w.retryPolicy.MaxDelay {
		return w.retryPolicy.MaxDelay
	}
	return delay
}

func getStackTrace() string {
	const maxStackFrames = 32
	var stack [maxStackFrames]uintptr
	n := runtime.Callers(3, stack[:])
	if n == 0 {
		return ""
	}
	frames := runtime.CallersFrames(stack[:n])
	var result string
	for {
		frame, more := frames.Next()
		result += fmt.Sprintf("\n\t%s:%d", frame.Function, frame.Line)
		if !more {
			break
		}
	}
	return result
}
