package deltask

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	taskcontext "github.com/gaoxin19/deltask/context"
	"github.com/gaoxin19/deltask/logger"
	"github.com/gaoxin19/deltask/task"
	"github.com/gaoxin19/deltask/testutil"
)

func TestNewTask(t *testing.T) {
	payload := map[string]any{"key": "value", "number": 42}

	t.Run("basic task creation", func(t *testing.T) {
		task := NewTask("test-task", payload)

		if task.Name != "test-task" {
			t.Errorf("NewTask() Name = %v, want 'test-task'", task.Name)
		}

		// 先检查 Payload 不为空
		if len(task.Payload) == 0 {
			t.Error("NewTask() should have non-empty payload")
		}

		// 验证 payload 通过反序列化
		var actualPayload map[string]any
		if err := json.Unmarshal(task.Payload, &actualPayload); err != nil {
			t.Errorf("Failed to unmarshal task payload: %v", err)
		} else {
			for k, v := range payload {
				actualValue := actualPayload[k]
				// JSON 反序列化会将数字转换为 float64
				if expectedInt, ok := v.(int); ok {
					if actualFloat, ok := actualValue.(float64); ok {
						if int(actualFloat) != expectedInt {
							t.Errorf("NewTask() payload[%s] = %v, want %v", k, int(actualFloat), expectedInt)
						}
					} else {
						t.Errorf("NewTask() payload[%s] is not a number: %T", k, actualValue)
					}
				} else if actualValue != v {
					t.Errorf("NewTask() payload[%s] = %v, want %v", k, actualValue, v)
				}
			}
		}

		if task.ID == "" {
			t.Error("NewTask() should generate a non-empty ID")
		}

		// ExecuteAt should be close to now
		if time.Since(task.ExecuteAt) > time.Second {
			t.Errorf("NewTask() ExecuteAt = %v, should be close to now", task.ExecuteAt)
		}
	})

	t.Run("task with delay", func(t *testing.T) {
		delay := 30 * time.Minute
		now := time.Now()
		task := NewTask("delayed-task", payload, task.WithDelay(delay))

		expectedTime := now.Add(delay)
		if task.ExecuteAt.Sub(expectedTime) > time.Second || expectedTime.Sub(task.ExecuteAt) > time.Second {
			t.Errorf("NewTask() ExecuteAt = %v, want around %v", task.ExecuteAt, expectedTime)
		}
	})
}

func TestNewClient(t *testing.T) {
	broker := testutil.NewMockBroker()
	client := NewClient(broker)

	if client.broker != broker {
		t.Error("NewClient() broker not set correctly")
	}
}

func TestClientPublish(t *testing.T) {
	broker := testutil.NewMockBroker()
	client := NewClient(broker)
	ctx := context.Background()

	task := NewTask("test-task", map[string]any{"key": "value"})
	queueName := "test-queue"

	err := client.Publish(ctx, task, queueName)
	if err != nil {
		t.Errorf("Client.Publish() error = %v", err)
	}

	if len(broker.PublishCalls) != 1 {
		t.Errorf("Client.Publish() publish calls = %v, want 1", len(broker.PublishCalls))
	}

	call := broker.PublishCalls[0]
	if call.Task != task {
		t.Error("Client.Publish() published wrong task")
	}
	if call.QueueName != queueName {
		t.Errorf("Client.Publish() queue name = %v, want %v", call.QueueName, queueName)
	}
}

func TestClientPublishWithNilTask(t *testing.T) {
	broker := testutil.NewMockBroker()
	client := NewClient(broker)
	ctx := context.Background()

	err := client.Publish(ctx, nil, "test-queue")
	if err == nil {
		t.Error("Client.Publish() with nil task should return error")
	}

	expectedError := "task cannot be nil"
	if err.Error() != expectedError {
		t.Errorf("Client.Publish() error = %v, want %v", err.Error(), expectedError)
	}

	// Should not call broker.Publish
	if len(broker.PublishCalls) != 0 {
		t.Errorf("Client.Publish() with nil task, publish calls = %v, want 0", len(broker.PublishCalls))
	}
}

func TestClientPublishWithEmptyQueueName(t *testing.T) {
	broker := testutil.NewMockBroker()
	client := NewClient(broker)
	ctx := context.Background()

	task := NewTask("test-task", nil)
	err := client.Publish(ctx, task, "")
	if err == nil {
		t.Error("Client.Publish() with empty queue name should return error")
	}

	expectedError := "queue name cannot be empty"
	if err.Error() != expectedError {
		t.Errorf("Client.Publish() error = %v, want %v", err.Error(), expectedError)
	}

	// Should not call broker.Publish
	if len(broker.PublishCalls) != 0 {
		t.Errorf("Client.Publish() with empty queue, publish calls = %v, want 0", len(broker.PublishCalls))
	}
}

func TestClientPublishWithBrokerError(t *testing.T) {
	broker := testutil.NewMockBroker()
	expectedError := errors.New("broker publish failed")
	broker.PublishFunc = func(ctx context.Context, t *task.Task, queueName string) error {
		return expectedError
	}

	client := NewClient(broker)
	ctx := context.Background()

	task := NewTask("test-task", nil)
	err := client.Publish(ctx, task, "test-queue")

	if err != expectedError {
		t.Errorf("Client.Publish() error = %v, want %v", err, expectedError)
	}

	// Should still call broker.Publish
	if len(broker.PublishCalls) != 1 {
		t.Errorf("Client.Publish() with broker error, publish calls = %v, want 1", len(broker.PublishCalls))
	}
}

func TestNewWorker(t *testing.T) {
	broker := testutil.NewMockBroker()
	queueName := "test-queue"
	concurrency := 3

	worker := NewWorker(broker, queueName, concurrency)

	if worker.w == nil {
		t.Error("NewWorker() internal worker not created")
	}

	// We can't directly access internal worker fields, but we can verify it was created
	// by testing the behavior through the public interface
}

func TestNewWorkerWithLogger(t *testing.T) {
	broker := testutil.NewMockBroker()
	queueName := "test-queue"
	concurrency := 2
	testLogger := logger.NewNopLogger()

	worker := NewWorker(broker, queueName, concurrency, WorkerOpts.WithLogger(testLogger))

	if worker.w == nil {
		t.Error("NewWorkerWithLogger() internal worker not created")
	}
}

func TestWorkerRegister(t *testing.T) {
	broker := testutil.NewMockBroker()
	worker := NewWorker(broker, "test-queue", 1)

	// 创建新的 Handler
	handler := func(ctx *taskcontext.Context) (any, error) {
		return "result", nil
	}

	// This should not panic
	worker.Register("test-task", handler)

	// We can't directly test the registration, but we can verify it doesn't panic
	// The actual functionality is tested through the internal worker tests
}

func TestWorkerRun(t *testing.T) {
	broker := testutil.NewMockBroker()
	worker := NewWorker(broker, "test-queue", 1)

	ctx, cancel := context.WithCancel(context.Background())

	// Cancel immediately to test that Run can handle cancellation
	cancel()

	err := worker.Run(ctx)
	if err != nil {
		t.Errorf("Worker.Run() error = %v", err)
	}

	// Should have called broker.Consume
	if len(broker.ConsumeCalls) != 1 {
		t.Errorf("Worker.Run() consume calls = %v, want 1", len(broker.ConsumeCalls))
	}

	if broker.ConsumeCalls[0].QueueName != "test-queue" {
		t.Errorf("Worker.Run() consumed from queue %v, want test-queue", broker.ConsumeCalls[0].QueueName)
	}
}

func TestWorkerRunWithConsumeError(t *testing.T) {
	broker := testutil.NewMockBroker()
	expectedError := errors.New("consume failed")
	broker.ConsumeFunc = func(ctx context.Context, queueName string) (<-chan *task.Task, error) {
		return nil, expectedError
	}

	worker := NewWorker(broker, "test-queue", 1)
	ctx := context.Background()

	err := worker.Run(ctx)
	if err == nil {
		t.Error("Worker.Run() with consume error should return error")
	}

	if !errors.Is(err, expectedError) {
		t.Errorf("Worker.Run() error should wrap the broker error, got %v", err)
	}
}

// Test type aliases
func TestTaskTypeAlias(t *testing.T) {
	// Test that Task is properly aliased
	var task Task = task.Task{
		ID:   "test",
		Name: "test-task",
	}

	if task.ID != "test" {
		t.Error("Task type alias not working correctly")
	}
}

func TestTaskHandlerTypeAlias(t *testing.T) {
	// Test that TaskHandler is properly aliased
	var handler TaskHandler = func(ctx *taskcontext.Context) (any, error) {
		return "result", nil
	}

	taskCtx := taskcontext.New(context.Background(), []byte("{}"), &taskcontext.TaskInfo{
		ID:    "test-id",
		Name:  "test-task",
		Retry: 0,
	})

	result, err := handler(taskCtx)
	if err != nil {
		t.Errorf("TaskHandler execution error = %v", err)
	}
	if result != "result" {
		t.Errorf("TaskHandler result = %v, want 'result'", result)
	}
}

func TestClientStructure(t *testing.T) {
	broker := testutil.NewMockBroker()
	client := &Client{broker: broker}

	// Test that Client struct has the expected fields
	if client.broker != broker {
		t.Error("Client struct broker field not accessible")
	}
}

func TestWorkerStructure(t *testing.T) {
	broker := testutil.NewMockBroker()
	worker := NewWorker(broker, "test-queue", 1)

	// Test that Worker struct has the expected fields
	if worker.w == nil {
		t.Error("Worker struct w field not accessible")
	}
}
