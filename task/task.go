package task

import (
	"encoding/json"
	"time"

	deltacontext "github.com/gaoxin19/deltask/context"
	"github.com/google/uuid"
)

// Task 是任务的定义，它是跨服务传递的基本单元。
type Task struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	Payload   []byte    `json:"-"` // 原始 JSON 字节数据，自定义序列化
	Retry     int       `json:"retry"`
	ExecuteAt time.Time `json:"execute_at"`

	// BrokerMeta 用于存储 Broker 实现所需的内部元数据，如 RabbitMQ 的 delivery tag。
	// 它不会被序列化。
	BrokerMeta any `json:"-"`
}

// taskOptions 包含了创建任务的所有可选配置项。
type taskOptions struct {
	id        string    // 自定义任务 ID
	executeAt time.Time // 执行时间
	retry     int       // 重试次数
}

// TaskOption 是一个函数类型，用于修改 taskOptions 结构体。
type TaskOption func(*taskOptions)

// newTaskOptions 创建一个带有默认值的 taskOptions 实例。
func newTaskOptions() *taskOptions {
	return &taskOptions{
		id:        uuid.NewString(),
		executeAt: time.Now(),
		retry:     0,
	}
}

// WithID 返回一个 TaskOption，用于设置任务的自定义 ID。
func WithID(id string) TaskOption {
	return func(o *taskOptions) {
		o.id = id
	}
}

// WithExecuteAt 返回一个 TaskOption，用于设置任务的执行时间。
func WithExecuteAt(executeAt time.Time) TaskOption {
	return func(o *taskOptions) {
		o.executeAt = executeAt
	}
}

// WithDelay 返回一个 TaskOption，用于设置任务的延迟执行时间。
func WithDelay(delay time.Duration) TaskOption {
	return func(o *taskOptions) {
		o.executeAt = time.Now().Add(delay)
	}
}

// WithRetry 返回一个 TaskOption，用于设置任务的初始重试次数。
func WithRetry(retry int) TaskOption {
	return func(o *taskOptions) {
		o.retry = retry
	}
}

// New 创建一个新任务，使用 options 模式进行配置。
func New(name string, payload map[string]any, opts ...TaskOption) *Task {
	cfg := newTaskOptions()
	for _, opt := range opts {
		opt(cfg)
	}

	// 将 payload 序列化为 JSON 字节
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		// 如果序列化失败，使用空对象
		payloadBytes = []byte("{}")
	}

	return &Task{
		ID:        cfg.id,
		Name:      name,
		Payload:   payloadBytes,
		ExecuteAt: cfg.executeAt,
		Retry:     cfg.retry,
	}
}

// NewWithRawPayload 创建一个具有原始 JSON 字节 payload 的新任务。
// 这个方法适用于已经有 JSON 数据的场景，避免额外的序列化开销。
func NewWithRawPayload(name string, payloadBytes []byte, opts ...TaskOption) *Task {
	cfg := newTaskOptions()
	for _, opt := range opts {
		opt(cfg)
	}

	return &Task{
		ID:        cfg.id,
		Name:      name,
		Payload:   payloadBytes,
		ExecuteAt: cfg.executeAt,
		Retry:     cfg.retry,
	}
}

// MarshalJSON 自定义 JSON 序列化，将 Payload 作为原始 JSON 嵌入
func (t *Task) MarshalJSON() ([]byte, error) {
	// 创建一个临时结构体，用于序列化除 Payload 外的所有字段
	type TaskAlias Task
	temp := struct {
		*TaskAlias
		Payload json.RawMessage `json:"payload"`
	}{
		TaskAlias: (*TaskAlias)(t),
		Payload:   json.RawMessage(t.Payload),
	}

	return json.Marshal(temp)
}

// UnmarshalJSON 自定义 JSON 反序列化，保持 Payload 为原始字节
func (t *Task) UnmarshalJSON(data []byte) error {
	// 创建一个临时结构体，用于反序列化
	type TaskAlias Task
	temp := struct {
		*TaskAlias
		Payload json.RawMessage `json:"payload"`
	}{
		TaskAlias: (*TaskAlias)(t),
	}

	if err := json.Unmarshal(data, &temp); err != nil {
		return err
	}

	// 将原始 JSON 字节保存到 Payload
	t.Payload = []byte(temp.Payload)
	return nil
}

// Handler 是任务处理函数类型，使用增强的 Context。
type Handler func(ctx *deltacontext.Context) (any, error)
