package deltask

import (
	"context"
	"errors"

	"github.com/gaoxin19/deltask/broker"
	"github.com/gaoxin19/deltask/internal"
	"github.com/gaoxin19/deltask/logger"
	"github.com/gaoxin19/deltask/task"
)

// Task 是任务的定义，它是 task.Task 的别名，方便用户使用。
type Task = task.Task

// NewTask 创建一个新任务，是对 task.New 的封装。
func NewTask(name string, payload map[string]any) *Task {
	return task.New(name, payload)
}

// TaskHandler 是任务处理函数的类型，它是 task.Handler 的别名。
type TaskHandler = task.Handler

// Client 是用于发布任务到 Broker 的客户端。
type Client struct {
	broker broker.Broker
}

// NewClient 创建一个新的任务发布客户端。
func NewClient(b broker.Broker) *Client {
	return &Client{broker: b}
}

// Publish 将一个任务发布到指定的队列。
func (c *Client) Publish(ctx context.Context, task *task.Task, queueName string) error {
	if task == nil {
		return errors.New("task cannot be nil")
	}
	if queueName == "" {
		return errors.New("queue name cannot be empty")
	}
	return c.broker.Publish(ctx, task, queueName)
}

// Worker 是任务处理的核心单元。
type Worker struct {
	w *internal.Worker
}

type RetryPolicy = internal.RetryPolicy

// workerOptions 包含了 Worker 的所有可选配置项。
type workerOptions struct {
	logger      *logger.Logger
	retryPolicy *internal.RetryPolicy
}

// WorkerOption 是一个函数类型，用于修改 workerOptions 结构体。
type WorkerOption func(*workerOptions)

// newWorkerOptions 创建一个带有默认值的 workerOptions 实例。
func newWorkerOptions() *workerOptions {
	defaultRetryPolicy := internal.DefaultRetryPolicy()
	return &workerOptions{
		logger:      logger.NewProductionLogger(),
		retryPolicy: &defaultRetryPolicy,
	}
}

// WorkerOpts 包含 Worker 相关的配置选项。
var WorkerOpts workerOptionBuilder

type workerOptionBuilder struct{}

// WithLogger 返回一个 WorkerOption，用于设置 Worker 的自定义 logger。
func (workerOptionBuilder) WithLogger(l *logger.Logger) WorkerOption {
	return func(o *workerOptions) {
		o.logger = l
	}
}

// WithRetryPolicy 返回一个 WorkerOption，用于设置 Worker 的重试策略。
func (workerOptionBuilder) WithRetryPolicy(policy RetryPolicy) WorkerOption {
	return func(o *workerOptions) {
		o.retryPolicy = &policy
	}
}

// NewWorker 创建一个新的 Worker 实例，使用 options 模式进行配置。
// broker: 消息中间件实例
// queueName: 队列名称
// concurrency: 并发处理任务的 goroutine 数量
// opts: 可选配置项
func NewWorker(broker broker.Broker, queueName string, concurrency int, opts ...WorkerOption) *Worker {
	cfg := newWorkerOptions()
	for _, opt := range opts {
		opt(cfg)
	}

	// 创建内部 Worker
	internalWorker := internal.NewWorkerWithLogger(broker, queueName, concurrency, cfg.logger)

	// 应用重试策略
	if cfg.retryPolicy != nil {
		internalWorker.WithRetryPolicy(*cfg.retryPolicy)
	}

	return &Worker{
		w: internalWorker,
	}
}

// Register 注册一个任务名称和对应的处理函数。
func (w *Worker) Register(taskName string, handler task.Handler) {
	w.w.Register(taskName, handler)
}

// Run 启动 Worker 开始消费和处理任务。
// 这是一个阻塞操作，它会一直运行直到传入的 context 被取消。
// 它会处理优雅关闭逻辑。
func (w *Worker) Run(ctx context.Context) error {
	return w.w.Run(ctx)
}

// Run 快速启动多个 Worker 实例，并处理关闭信号。
// 这是一个便捷函数，内部使用 Manager 来管理 Worker 生命周期。
func Run(ctx context.Context, workers []*Worker, opts ...ManagerOption) error {
	manager := NewManager(workers, opts...)
	return manager.Run(ctx)
}
