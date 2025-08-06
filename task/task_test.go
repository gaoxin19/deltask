package task

import (
	"context"
	"testing"
	"time"

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
			if tt.payload == nil && task.Payload != nil {
				t.Errorf("New() payload = %v, want nil", task.Payload)
			} else if tt.payload != nil {
				if len(task.Payload) != len(tt.payload) {
					t.Errorf("New() payload length = %v, want %v", len(task.Payload), len(tt.payload))
				}
				for k, v := range tt.payload {
					if task.Payload[k] != v {
						t.Errorf("New() payload[%s] = %v, want %v", k, task.Payload[k], v)
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
	// Test that Handler type can be used correctly
	var handler Handler = func(ctx context.Context, payload map[string]any) (any, error) {
		return "result", nil
	}

	ctx := context.Background()
	payload := map[string]any{"test": "data"}

	result, err := handler(ctx, payload)
	if err != nil {
		t.Errorf("Handler execution failed: %v", err)
	}

	if result != "result" {
		t.Errorf("Handler result = %v, want 'result'", result)
	}
}

func TestTaskStructFields(t *testing.T) {
	task := &Task{
		ID:         "test-id",
		Name:       "test-task",
		Payload:    map[string]any{"key": "value"},
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
	if task.Payload["key"] != "value" {
		t.Errorf("Task.Payload[key] = %v, want 'value'", task.Payload["key"])
	}
	if task.Retry != 3 {
		t.Errorf("Task.Retry = %v, want 3", task.Retry)
	}
	if task.BrokerMeta != "some-meta" {
		t.Errorf("Task.BrokerMeta = %v, want 'some-meta'", task.BrokerMeta)
	}
}
