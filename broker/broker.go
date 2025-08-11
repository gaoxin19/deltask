package broker

import (
	"context"
	"io"

	"github.com/gaoxin19/deltask/task"
)

type ConsumeOptions struct {
	PrefetchCount int // 预取数量
}

type ConsumeOption func(o *ConsumeOptions)

func WithPrefetchCount(prefetchCount int) ConsumeOption {
	return func(o *ConsumeOptions) {
		o.PrefetchCount = prefetchCount
	}
}

// Broker 定义了与消息中间件交互的接口。
type Broker interface {
	io.Closer
	Publish(ctx context.Context, t *task.Task, queueName string) error
	Consume(ctx context.Context, queueName string, opts ...ConsumeOption) (<-chan *task.Task, error)
	Ack(ctx context.Context, t *task.Task) error
	Nack(ctx context.Context, t *task.Task, requeue bool) error
}
