package task

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	taskcontext "github.com/gaoxin19/deltask/context"
	"github.com/google/uuid"
)

func TestNew(t *testing.T) {
	tests := []struct {
		name     string
		taskName string
		payload  map[string]any
	}{
		{
			name:     "creates task with string payload",
			taskName: "test_task",
			payload:  map[string]any{"key": "value"},
		},
		{
			name:     "creates task with mixed payload",
			taskName: "mixed_task",
			payload:  map[string]any{"str": "value", "int": 42, "bool": true},
		},
		{
			name:     "creates task with nil payload",
			taskName: "nil_task",
			payload:  nil,
		},
		{
			name:     "creates task with empty payload",
			taskName: "empty_task",
			payload:  map[string]any{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			task := New(tt.taskName, tt.payload)

			// Verify task properties
			if task.Name != tt.taskName {
				t.Errorf("New() name = %v, want %v", task.Name, tt.taskName)
			}

			// Verify payload
			// 验证 Payload 是否正确序列化
			if tt.payload == nil {
				// nil payload 应该序列化为 {}
				var actualPayload map[string]any
				if err := json.Unmarshal(task.Payload, &actualPayload); err != nil {
					t.Errorf("Failed to unmarshal nil payload: %v", err)
				} else if len(actualPayload) != 0 {
					t.Errorf("New() nil payload should be empty object, got %v", actualPayload)
				}
			} else {
				// 反序列化 payload 并比较
				var actualPayload map[string]any
				if err := json.Unmarshal(task.Payload, &actualPayload); err != nil {
					t.Errorf("Failed to unmarshal payload: %v", err)
				} else {
					if len(actualPayload) != len(tt.payload) {
						t.Errorf("New() payload length = %v, want %v", len(actualPayload), len(tt.payload))
					}
					for k, v := range tt.payload {
						actualValue := actualPayload[k]
						// JSON 反序列化会将数字转换为 float64，需要特殊处理
						if expectedInt, ok := v.(int); ok {
							if actualFloat, ok := actualValue.(float64); ok {
								if int(actualFloat) != expectedInt {
									t.Errorf("New() payload[%s] = %v, want %v", k, int(actualFloat), expectedInt)
								}
							} else {
								t.Errorf("New() payload[%s] is not a number: %T", k, actualValue)
							}
						} else if actualValue != v {
							t.Errorf("New() payload[%s] = %v, want %v", k, actualValue, v)
						}
					}
				}
			}

			// Verify ID is valid UUID
			if _, err := uuid.Parse(task.ID); err != nil {
				t.Errorf("New() generated invalid UUID: %v", err)
			}

			// Verify retry count starts at 0
			if task.Retry != 0 {
				t.Errorf("New() retry = %v, want 0", task.Retry)
			}

			// Verify ExecuteAt is close to now
			now := time.Now()
			if task.ExecuteAt.After(now.Add(time.Second)) || task.ExecuteAt.Before(now.Add(-time.Second)) {
				t.Errorf("New() ExecuteAt = %v, should be close to %v", task.ExecuteAt, now)
			}

			// Verify BrokerMeta is nil
			if task.BrokerMeta != nil {
				t.Errorf("New() BrokerMeta = %v, want nil", task.BrokerMeta)
			}
		})
	}
}

func TestHandler(t *testing.T) {
	// Test that new Handler type can be used correctly
	var handler Handler = func(ctx *taskcontext.Context) (any, error) {
		return "result", nil
	}

	payloadBytes, _ := json.Marshal(map[string]any{"test": "data"})
	taskCtx := taskcontext.New(context.Background(), payloadBytes, &taskcontext.TaskInfo{
		ID:    "test-id",
		Name:  "test-task",
		Retry: 0,
	})

	result, err := handler(taskCtx)
	if err != nil {
		t.Errorf("Handler execution failed: %v", err)
	}

	if result != "result" {
		t.Errorf("Handler result = %v, want 'result'", result)
	}
}

func TestNewWithOptions(t *testing.T) {
	payload := map[string]any{"test": "data"}

	t.Run("default options", func(t *testing.T) {
		task := New("test-task", payload)

		if task.Name != "test-task" {
			t.Errorf("Name = %v, want 'test-task'", task.Name)
		}
		if task.Retry != 0 {
			t.Errorf("Retry = %v, want 0", task.Retry)
		}
		if task.ID == "" {
			t.Error("ID should not be empty")
		}
	})

	t.Run("with custom ID", func(t *testing.T) {
		customID := "custom-123"
		task := New("test-task", payload, WithID(customID))

		if task.ID != customID {
			t.Errorf("ID = %v, want %v", task.ID, customID)
		}
	})

	t.Run("with delay", func(t *testing.T) {
		delay := 5 * time.Minute
		now := time.Now()
		task := New("test-task", payload, WithDelay(delay))

		expectedTime := now.Add(delay)
		// 允许1秒的误差
		if task.ExecuteAt.Sub(expectedTime) > time.Second || expectedTime.Sub(task.ExecuteAt) > time.Second {
			t.Errorf("ExecuteAt = %v, want around %v", task.ExecuteAt, expectedTime)
		}
	})

	t.Run("with specific execute time", func(t *testing.T) {
		executeAt := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
		task := New("test-task", payload, WithExecuteAt(executeAt))

		if !task.ExecuteAt.Equal(executeAt) {
			t.Errorf("ExecuteAt = %v, want %v", task.ExecuteAt, executeAt)
		}
	})

	t.Run("with retry", func(t *testing.T) {
		retry := 3
		task := New("test-task", payload, WithRetry(retry))

		if task.Retry != retry {
			t.Errorf("Retry = %v, want %v", task.Retry, retry)
		}
	})

	t.Run("with multiple options", func(t *testing.T) {
		customID := "multi-123"
		delay := 2 * time.Hour
		retry := 5
		now := time.Now()

		task := New("test-task", payload,
			WithID(customID),
			WithDelay(delay),
			WithRetry(retry))

		if task.ID != customID {
			t.Errorf("ID = %v, want %v", task.ID, customID)
		}
		if task.Retry != retry {
			t.Errorf("Retry = %v, want %v", task.Retry, retry)
		}

		expectedTime := now.Add(delay)
		if task.ExecuteAt.Sub(expectedTime) > time.Second || expectedTime.Sub(task.ExecuteAt) > time.Second {
			t.Errorf("ExecuteAt = %v, want around %v", task.ExecuteAt, expectedTime)
		}
	})

	t.Run("using TaskOpts builder", func(t *testing.T) {
		delay := 10 * time.Minute
		now := time.Now()

		task := New("test-task", payload,
			WithDelay(delay),
			WithRetry(2))

		if task.Retry != 2 {
			t.Errorf("Retry = %v, want 2", task.Retry)
		}

		expectedTime := now.Add(delay)
		if task.ExecuteAt.Sub(expectedTime) > time.Second || expectedTime.Sub(task.ExecuteAt) > time.Second {
			t.Errorf("ExecuteAt = %v, want around %v", task.ExecuteAt, expectedTime)
		}
	})
}

func TestTaskStructFields(t *testing.T) {
	payloadBytes, _ := json.Marshal(map[string]any{"key": "value"})
	task := &Task{
		ID:         "test-id",
		Name:       "test-task",
		Payload:    payloadBytes,
		Retry:      3,
		ExecuteAt:  time.Now().Add(5 * time.Minute),
		BrokerMeta: "some-meta",
	}

	// Verify all fields are accessible
	if task.ID != "test-id" {
		t.Errorf("Task.ID = %v, want 'test-id'", task.ID)
	}
	if task.Name != "test-task" {
		t.Errorf("Task.Name = %v, want 'test-task'", task.Name)
	}
	// Verify Payload by unmarshaling
	var payload map[string]any
	if err := json.Unmarshal(task.Payload, &payload); err != nil {
		t.Errorf("Failed to unmarshal task payload: %v", err)
	} else if payload["key"] != "value" {
		t.Errorf("Task.Payload['key'] = %v, want 'value'", payload["key"])
	}
	if task.Retry != 3 {
		t.Errorf("Task.Retry = %v, want 3", task.Retry)
	}
	if task.BrokerMeta != "some-meta" {
		t.Errorf("Task.BrokerMeta = %v, want 'some-meta'", task.BrokerMeta)
	}
}
