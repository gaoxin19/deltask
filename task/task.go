package task

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// Task 是任务的定义，它是跨服务传递的基本单元。
type Task struct {
	ID        string         `json:"id"`
	Name      string         `json:"name"`
	Payload   map[string]any `json:"payload"`
	Retry     int            `json:"retry"`
	ExecuteAt time.Time      `json:"execute_at"`

	// BrokerMeta 用于存储 Broker 实现所需的内部元数据，如 RabbitMQ 的 delivery tag。
	// 它不会被序列化。
	BrokerMeta any `json:"-"`
}

// New 创建一个具有唯一 ID 和默认即时执行的新任务。
func New(name string, payload map[string]any) *Task {
	return &Task{
		ID:        uuid.NewString(),
		Name:      name,
		Payload:   payload,
		ExecuteAt: time.Now(),
	}
}

// Handler 是任务处理函数的类型。
type Handler func(ctx context.Context, payload map[string]any) (any, error)
