package internal

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/gaoxin19/deltask/broker"
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

	msgChan, err := w.broker.Consume(workerCtx, w.queueName, brokerConsumeOptions...)
	if err != nil {
		return fmt.Errorf("failed to start consuming: %w", err)
	}

	w.logger.Info("Starting workers for queue",
		zap.Int("concurrency", w.concurrency),
		zap.String("queue", w.queueName))

	for i := range w.concurrency {
		w.wg.Add(1)
		go func(workerID int) {
			defer w.wg.Done()
			w.logger.Info("Worker started", zap.Int("worker_id", workerID))
			for {
				select {
				case <-workerCtx.Done():
					w.logger.Info("Worker shutting down", zap.Int("worker_id", workerID))
					return
				case t, ok := <-msgChan:
					if !ok {
						w.logger.Info("Worker stopping, message channel closed", zap.Int("worker_id", workerID))
						return
					}
					// 使用worker上下文，允许任务取消
					w.processTask(workerCtx, t)
				}
			}
		}(i + 1)
	}

	w.wg.Wait()
	w.logger.Info("All workers have shut down")
	return nil
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
	handler, ok := w.registry[t.Name]
	if !ok {
		w.handleUnknownTask(ctx, t)
		return
	}

	w.logTaskStart(t)

	_, err := handler(ctx, t.Payload)
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
		w.logger.Error("Failed to Ack successful task",
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
		w.logger.Error("Failed to Ack retried task",
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
