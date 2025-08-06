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

// NewWorker 创建一个新的 Worker 实例。
// concurrency 指定了并发处理任务的 goroutine 数量。
func NewWorker(b broker.Broker, queueName string, concurrency int) *Worker {
	return &Worker{
		w: internal.NewWorker(b, queueName, concurrency),
	}
}

// NewWorkerWithLogger 创建一个带有指定logger的 Worker 实例。
// concurrency 指定了并发处理任务的 goroutine 数量。
func NewWorkerWithLogger(b broker.Broker, queueName string, concurrency int, log *logger.Logger) *Worker {
	return &Worker{
		w: internal.NewWorkerWithLogger(b, queueName, concurrency, log),
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
