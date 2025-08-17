package internal

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	deltacontext "github.com/gaoxin19/deltask/context"
	"github.com/gaoxin19/deltask/logger"
	"github.com/gaoxin19/deltask/task"
	"github.com/gaoxin19/deltask/testutil"
)

func TestDefaultRetryPolicy(t *testing.T) {
	policy := DefaultRetryPolicy()

	if policy.MaxRetries != 3 {
		t.Errorf("DefaultRetryPolicy() MaxRetries = %v, want 3", policy.MaxRetries)
	}
	if policy.BaseDelay != time.Second {
		t.Errorf("DefaultRetryPolicy() BaseDelay = %v, want 1s", policy.BaseDelay)
	}
	if policy.MaxDelay != time.Minute {
		t.Errorf("DefaultRetryPolicy() MaxDelay = %v, want 1m", policy.MaxDelay)
	}
	if !policy.Exponential {
		t.Error("DefaultRetryPolicy() Exponential = false, want true")
	}
}

func TestNewWorker(t *testing.T) {
	broker := testutil.NewMockBroker()
	worker := NewWorker(broker, "test-queue", 2)

	if worker.broker != broker {
		t.Error("NewWorker() broker not set correctly")
	}
	if worker.queueName != "test-queue" {
		t.Errorf("NewWorker() queueName = %v, want 'test-queue'", worker.queueName)
	}
	if worker.concurrency != 2 {
		t.Errorf("NewWorker() concurrency = %v, want 2", worker.concurrency)
	}
	if worker.registry == nil {
		t.Error("NewWorker() registry not initialized")
	}
	if worker.logger == nil {
		t.Error("NewWorker() logger not set")
	}
}

func TestNewWorkerWithLogger(t *testing.T) {
	broker := testutil.NewMockBroker()
	testLogger := logger.NewNopLogger()
	worker := NewWorkerWithLogger(broker, "test-queue", 2, testLogger)

	if worker.logger != testLogger {
		t.Error("NewWorkerWithLogger() logger not set correctly")
	}
}

func TestNewWorkerWithZeroConcurrency(t *testing.T) {
	broker := testutil.NewMockBroker()
	worker := NewWorker(broker, "test-queue", 0)

	if worker.concurrency != 1 {
		t.Errorf("NewWorker() with zero concurrency should default to 1, got %v", worker.concurrency)
	}
}

func TestNewWorkerWithNilLogger(t *testing.T) {
	broker := testutil.NewMockBroker()
	worker := NewWorkerWithLogger(broker, "test-queue", 1, nil)

	if worker.logger == nil {
		t.Error("NewWorkerWithLogger() with nil logger should set default logger")
	}
}

func TestWorkerWithRetryPolicy(t *testing.T) {
	broker := testutil.NewMockBroker()
	worker := NewWorker(broker, "test-queue", 1)

	customPolicy := RetryPolicy{
		MaxRetries:  5,
		BaseDelay:   2 * time.Second,
		MaxDelay:    10 * time.Minute,
		Exponential: false,
	}

	result := worker.WithRetryPolicy(customPolicy)

	if result != worker {
		t.Error("WithRetryPolicy() should return the same worker instance")
	}
	if worker.retryPolicy.MaxRetries != 5 {
		t.Errorf("WithRetryPolicy() MaxRetries = %v, want 5", worker.retryPolicy.MaxRetries)
	}
	if worker.retryPolicy.BaseDelay != 2*time.Second {
		t.Errorf("WithRetryPolicy() BaseDelay = %v, want 2s", worker.retryPolicy.BaseDelay)
	}
}

func TestWorkerRegister(t *testing.T) {
	broker := testutil.NewMockBroker()
	worker := NewWorker(broker, "test-queue", 1)

	handler := func(ctx *deltacontext.Context) (any, error) {
		return "result", nil
	}

	worker.Register("test-task", handler)

	if len(worker.registry) != 1 {
		t.Errorf("Register() registry length = %v, want 1", len(worker.registry))
	}

	registeredHandler, exists := worker.registry["test-task"]
	if !exists {
		t.Error("Register() task not found in registry")
	}

	// Test the registered handler
	taskCtx := deltacontext.New(context.Background(), []byte("{}"), &deltacontext.TaskInfo{
		ID:    "test-id",
		Name:  "test-task",
		Retry: 0,
	})
	result, err := registeredHandler(taskCtx)
	if err != nil {
		t.Errorf("Registered handler error = %v", err)
	}
	if result != "result" {
		t.Errorf("Registered handler result = %v, want 'result'", result)
	}
}

func TestCalculateRetryDelay(t *testing.T) {
	tests := []struct {
		name       string
		policy     RetryPolicy
		retryCount int
		wantDelay  time.Duration
	}{
		{
			name: "non-exponential delay",
			policy: RetryPolicy{
				BaseDelay:   2 * time.Second,
				MaxDelay:    10 * time.Second,
				Exponential: false,
			},
			retryCount: 3,
			wantDelay:  2 * time.Second,
		},
		{
			name: "exponential delay - first retry",
			policy: RetryPolicy{
				BaseDelay:   time.Second,
				MaxDelay:    10 * time.Second,
				Exponential: true,
			},
			retryCount: 1,
			wantDelay:  2 * time.Second, // 1 * 2^1
		},
		{
			name: "exponential delay - second retry",
			policy: RetryPolicy{
				BaseDelay:   time.Second,
				MaxDelay:    10 * time.Second,
				Exponential: true,
			},
			retryCount: 2,
			wantDelay:  4 * time.Second, // 1 * 2^2
		},
		{
			name: "exponential delay - capped at max",
			policy: RetryPolicy{
				BaseDelay:   time.Second,
				MaxDelay:    5 * time.Second,
				Exponential: true,
			},
			retryCount: 10, // Would be 1024s without cap
			wantDelay:  5 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			broker := testutil.NewMockBroker()
			worker := NewWorker(broker, "test-queue", 1).WithRetryPolicy(tt.policy)

			delay := worker.calculateRetryDelay(tt.retryCount)

			if delay != tt.wantDelay {
				t.Errorf("calculateRetryDelay() = %v, want %v", delay, tt.wantDelay)
			}
		})
	}
}

func TestShouldMoveToDeadLetter(t *testing.T) {
	broker := testutil.NewMockBroker()
	worker := NewWorker(broker, "test-queue", 1)

	tests := []struct {
		name       string
		taskRetry  int
		maxRetries int
		want       bool
	}{
		{"below max retries", 2, 3, false},
		{"at max retries", 3, 3, true},
		{"above max retries", 4, 3, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			worker.retryPolicy.MaxRetries = tt.maxRetries
			task := &task.Task{Retry: tt.taskRetry}

			if got := worker.shouldMoveToDeadLetter(task); got != tt.want {
				t.Errorf("shouldMoveToDeadLetter() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCreateRetryTask(t *testing.T) {
	broker := testutil.NewMockBroker()
	worker := NewWorker(broker, "test-queue", 1)

	originalTask := &task.Task{
		ID:      "test-id",
		Name:    "test-task",
		Payload: []byte(`{"key":"value"}`),
		Retry:   2,
	}

	delay := 5 * time.Second
	retryTask := worker.createRetryTask(originalTask, delay)

	if retryTask.ID != originalTask.ID {
		t.Errorf("createRetryTask() ID = %v, want %v", retryTask.ID, originalTask.ID)
	}
	if retryTask.Name != originalTask.Name {
		t.Errorf("createRetryTask() Name = %v, want %v", retryTask.Name, originalTask.Name)
	}
	if retryTask.Retry != originalTask.Retry+1 {
		t.Errorf("createRetryTask() Retry = %v, want %v", retryTask.Retry, originalTask.Retry+1)
	}

	// Check that ExecuteAt is approximately now + delay
	expectedTime := time.Now().Add(delay)
	if retryTask.ExecuteAt.Before(expectedTime.Add(-time.Second)) ||
		retryTask.ExecuteAt.After(expectedTime.Add(time.Second)) {
		t.Errorf("createRetryTask() ExecuteAt = %v, should be around %v", retryTask.ExecuteAt, expectedTime)
	}
}

func TestHandleTaskSuccess(t *testing.T) {
	broker := testutil.NewMockBroker()
	worker := NewWorkerWithLogger(broker, "test-queue", 1, logger.NewNopLogger())

	testTask := &task.Task{ID: "test-id", Name: "test-task"}
	ctx := context.Background()

	worker.handleTaskSuccess(ctx, testTask)

	if len(broker.AckCalls) != 1 {
		t.Errorf("handleTaskSuccess() ack calls = %v, want 1", len(broker.AckCalls))
	}
	if broker.AckCalls[0].Task != testTask {
		t.Error("handleTaskSuccess() acked wrong task")
	}
}

func TestHandleUnknownTask(t *testing.T) {
	broker := testutil.NewMockBroker()
	worker := NewWorkerWithLogger(broker, "test-queue", 1, logger.NewNopLogger())

	testTask := &task.Task{ID: "test-id", Name: "unknown-task"}
	ctx := context.Background()

	worker.handleUnknownTask(ctx, testTask)

	if len(broker.NackCalls) != 1 {
		t.Errorf("handleUnknownTask() nack calls = %v, want 1", len(broker.NackCalls))
	}
	if broker.NackCalls[0].Task != testTask {
		t.Error("handleUnknownTask() nacked wrong task")
	}
	if broker.NackCalls[0].Requeue != false {
		t.Error("handleUnknownTask() should nack with requeue=false")
	}
}

func TestHandleDeadLetterTask(t *testing.T) {
	broker := testutil.NewMockBroker()
	worker := NewWorkerWithLogger(broker, "test-queue", 1, logger.NewNopLogger())

	testTask := &task.Task{ID: "test-id", Name: "test-task", Retry: 3}
	ctx := context.Background()

	worker.handleDeadLetterTask(ctx, testTask)

	if len(broker.NackCalls) != 1 {
		t.Errorf("handleDeadLetterTask() nack calls = %v, want 1", len(broker.NackCalls))
	}
	if broker.NackCalls[0].Requeue != false {
		t.Error("handleDeadLetterTask() should nack with requeue=false")
	}
}

func TestRetryTask(t *testing.T) {
	broker := testutil.NewMockBroker()
	worker := NewWorkerWithLogger(broker, "test-queue", 1, logger.NewNopLogger())

	testTask := &task.Task{
		ID:    "test-id",
		Name:  "test-task",
		Retry: 1,
	}
	ctx := context.Background()

	worker.retryTask(ctx, testTask)

	// Should publish retry task
	if len(broker.PublishCalls) != 1 {
		t.Errorf("retryTask() publish calls = %v, want 1", len(broker.PublishCalls))
	}

	publishedTask := broker.PublishCalls[0].Task
	if publishedTask.Retry != testTask.Retry+1 {
		t.Errorf("retryTask() published task retry = %v, want %v", publishedTask.Retry, testTask.Retry+1)
	}

	// Should ack original task
	if len(broker.AckCalls) != 1 {
		t.Errorf("retryTask() ack calls = %v, want 1", len(broker.AckCalls))
	}
	if broker.AckCalls[0].Task != testTask {
		t.Error("retryTask() acked wrong task")
	}
}

func TestRetryTaskPublishFailure(t *testing.T) {
	broker := testutil.NewMockBroker()
	broker.PublishFunc = func(ctx context.Context, t *task.Task, queueName string) error {
		return errors.New("publish failed")
	}

	worker := NewWorkerWithLogger(broker, "test-queue", 1, logger.NewNopLogger())

	testTask := &task.Task{
		ID:    "test-id",
		Name:  "test-task",
		Retry: 1,
	}
	ctx := context.Background()

	worker.retryTask(ctx, testTask)

	// Should nack original task when publish fails
	if len(broker.NackCalls) != 1 {
		t.Errorf("retryTask() with publish failure, nack calls = %v, want 1", len(broker.NackCalls))
	}
	if broker.NackCalls[0].Requeue != false {
		t.Error("retryTask() with publish failure should nack with requeue=false")
	}
}

func TestProcessTaskWithSuccess(t *testing.T) {
	broker := testutil.NewMockBroker()
	worker := NewWorkerWithLogger(broker, "test-queue", 1, logger.NewNopLogger())

	// Register a successful handler
	worker.Register("test-task", func(ctx *deltacontext.Context) (any, error) {
		return "success", nil
	})

	testTask := &task.Task{
		ID:   "test-id",
		Name: "test-task",
	}
	ctx := context.Background()

	worker.processTask(ctx, testTask)

	// Should ack the successful task
	if len(broker.AckCalls) != 1 {
		t.Errorf("processTask() with success, ack calls = %v, want 1", len(broker.AckCalls))
	}
}

func TestProcessTaskWithFailure(t *testing.T) {
	broker := testutil.NewMockBroker()
	worker := NewWorkerWithLogger(broker, "test-queue", 1, logger.NewNopLogger())

	// Register a failing handler
	worker.Register("test-task", func(ctx *deltacontext.Context) (any, error) {
		return nil, errors.New("task failed")
	})

	testTask := &task.Task{
		ID:    "test-id",
		Name:  "test-task",
		Retry: 0,
	}
	ctx := context.Background()

	worker.processTask(ctx, testTask)

	// Should publish retry task and ack original
	if len(broker.PublishCalls) != 1 {
		t.Errorf("processTask() with failure, publish calls = %v, want 1", len(broker.PublishCalls))
	}
	if len(broker.AckCalls) != 1 {
		t.Errorf("processTask() with failure, ack calls = %v, want 1", len(broker.AckCalls))
	}
}

func TestProcessTaskWithUnknownHandler(t *testing.T) {
	broker := testutil.NewMockBroker()
	worker := NewWorkerWithLogger(broker, "test-queue", 1, logger.NewNopLogger())

	testTask := &task.Task{
		ID:   "test-id",
		Name: "unknown-task",
	}
	ctx := context.Background()

	worker.processTask(ctx, testTask)

	// Should nack unknown task
	if len(broker.NackCalls) != 1 {
		t.Errorf("processTask() with unknown handler, nack calls = %v, want 1", len(broker.NackCalls))
	}
	if broker.NackCalls[0].Requeue != false {
		t.Error("processTask() with unknown handler should nack with requeue=false")
	}
}

func TestWorkerRun(t *testing.T) {
	broker := testutil.NewMockBroker()
	worker := NewWorkerWithLogger(broker, "test-queue", 1, logger.NewNopLogger())

	// Setup a channel that we can control
	taskChan := make(chan *task.Task, 1)
	testTask := &task.Task{ID: "test-id", Name: "test-task"}
	taskChan <- testTask
	close(taskChan)

	broker.ConsumeFunc = func(ctx context.Context, queueName string) (<-chan *task.Task, error) {
		return taskChan, nil
	}

	// Register a handler
	handlerCalled := false
	worker.Register("test-task", func(ctx *deltacontext.Context) (any, error) {
		handlerCalled = true
		return "success", nil
	})

	// Create a context that will be cancelled
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Cancel after a short delay to stop the worker
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	err := worker.Run(ctx)
	if err != nil {
		t.Errorf("Worker.Run() error = %v", err)
	}

	// Verify the handler was called
	if !handlerCalled {
		t.Error("Handler should have been called")
	}

	// Verify task was acked
	if len(broker.AckCalls) != 1 {
		t.Errorf("Expected 1 ack call, got %v", len(broker.AckCalls))
	}
}

func TestWorkerStop(t *testing.T) {
	broker := testutil.NewMockBroker()
	worker := NewWorkerWithLogger(broker, "test-queue", 1, logger.NewNopLogger())

	// Setup consume function
	broker.ConsumeFunc = func(ctx context.Context, queueName string) (<-chan *task.Task, error) {
		return make(<-chan *task.Task), nil
	}

	// Start worker in goroutine
	ctx := context.Background()
	go func() {
		worker.Run(ctx)
	}()

	// Give it a moment to start
	time.Sleep(10 * time.Millisecond)

	// Stop the worker
	worker.Stop()

	// Multiple stops should be safe
	worker.Stop()
}

func TestWorkerWithMultipleConcurrency(t *testing.T) {
	broker := testutil.NewMockBroker()
	worker := NewWorkerWithLogger(broker, "test-queue", 3, logger.NewNopLogger())

	// Setup a channel with multiple tasks
	taskChan := make(chan *task.Task, 3)
	for i := 0; i < 3; i++ {
		taskChan <- &task.Task{
			ID:   fmt.Sprintf("test-id-%d", i),
			Name: "test-task",
		}
	}
	close(taskChan)

	broker.ConsumeFunc = func(ctx context.Context, queueName string) (<-chan *task.Task, error) {
		return taskChan, nil
	}

	// Register a handler
	handlerCallCount := 0
	var mu sync.Mutex
	worker.Register("test-task", func(ctx *deltacontext.Context) (any, error) {
		mu.Lock()
		handlerCallCount++
		mu.Unlock()
		return "success", nil
	})

	// Create a context that will be cancelled
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Cancel after a short delay
	go func() {
		time.Sleep(200 * time.Millisecond)
		cancel()
	}()

	err := worker.Run(ctx)
	if err != nil {
		t.Errorf("Worker.Run() error = %v", err)
	}

	// All tasks should have been processed
	mu.Lock()
	if handlerCallCount != 3 {
		t.Errorf("Expected 3 handler calls, got %v", handlerCallCount)
	}
	mu.Unlock()

	// All tasks should have been acked
	if len(broker.AckCalls) != 3 {
		t.Errorf("Expected 3 ack calls, got %v", len(broker.AckCalls))
	}
}

func TestHandleRetryPublishFailure(t *testing.T) {
	broker := testutil.NewMockBroker()
	worker := NewWorkerWithLogger(broker, "test-queue", 1, logger.NewNopLogger())

	testTask := &task.Task{
		ID:    "test-id",
		Name:  "test-task",
		Retry: 1,
	}
	ctx := context.Background()
	publishError := errors.New("publish failed")

	worker.handleRetryPublishFailure(ctx, testTask, publishError)

	// Should nack the task
	if len(broker.NackCalls) != 1 {
		t.Errorf("handleRetryPublishFailure() nack calls = %v, want 1", len(broker.NackCalls))
	}
	if broker.NackCalls[0].Requeue != false {
		t.Error("handleRetryPublishFailure() should nack with requeue=false")
	}
}

func TestLogMethods(t *testing.T) {
	broker := testutil.NewMockBroker()
	worker := NewWorkerWithLogger(broker, "test-queue", 1, logger.NewNopLogger())

	testTask := &task.Task{
		ID:    "test-id",
		Name:  "test-task",
		Retry: 1,
	}

	// These should not panic
	worker.logTaskStart(testTask)
	worker.logTaskFailure(testTask, errors.New("test error"))
	worker.logRetryAttempt(testTask, 5*time.Second, 2)
}

func TestProcessTaskWithPanic(t *testing.T) {
	broker := testutil.NewMockBroker()
	worker := NewWorkerWithLogger(broker, "test-queue", 1, logger.NewNopLogger())

	// 注册一个会panic的handler
	worker.Register("panic-task", func(ctx *deltacontext.Context) (any, error) {
		panic("intentional panic for testing")
	})

	testTask := &task.Task{
		ID:    "test-id",
		Name:  "panic-task",
		Retry: 0,
	}
	ctx := context.Background()

	// 这不应该panic，应该被recover捕获
	worker.processTask(ctx, testTask)

	// 应该发布重试任务，因为panic被视为任务失败
	if len(broker.PublishCalls) != 1 {
		t.Errorf("processTask() with panic, publish calls = %v, want 1", len(broker.PublishCalls))
	}

	// 应该ack原任务（因为重试任务已发布）
	if len(broker.AckCalls) != 1 {
		t.Errorf("processTask() with panic, ack calls = %v, want 1", len(broker.AckCalls))
	}
}

func TestProcessTaskWithNilPointerPanic(t *testing.T) {
	broker := testutil.NewMockBroker()
	worker := NewWorkerWithLogger(broker, "test-queue", 1, logger.NewNopLogger())

	// 注册一个会导致nil pointer dereference的handler
	worker.Register("nil-pointer-task", func(ctx *deltacontext.Context) (any, error) {
		var ptr *int
		*ptr = 42 // 这会导致panic
		return nil, nil
	})

	testTask := &task.Task{
		ID:    "test-id",
		Name:  "nil-pointer-task",
		Retry: 0,
	}
	ctx := context.Background()

	// 这不应该panic，应该被recover捕获
	worker.processTask(ctx, testTask)

	// 应该发布重试任务
	if len(broker.PublishCalls) != 1 {
		t.Errorf("processTask() with nil pointer panic, publish calls = %v, want 1", len(broker.PublishCalls))
	}
}

func TestProcessTaskWithPanicExceedsMaxRetries(t *testing.T) {
	broker := testutil.NewMockBroker()
	worker := NewWorkerWithLogger(broker, "test-queue", 1, logger.NewNopLogger())

	// 注册一个会panic的handler
	worker.Register("panic-task", func(ctx *deltacontext.Context) (any, error) {
		panic("intentional panic for testing")
	})

	testTask := &task.Task{
		ID:    "test-id",
		Name:  "panic-task",
		Retry: 3, // 已经达到最大重试次数
	}
	ctx := context.Background()

	// 这不应该panic，应该被recover捕获
	worker.processTask(ctx, testTask)

	// 应该nack任务（不重新入队），因为超过最大重试次数
	if len(broker.NackCalls) != 1 {
		t.Errorf("processTask() with panic exceeding max retries, nack calls = %v, want 1", len(broker.NackCalls))
	}
	if broker.NackCalls[0].Requeue != false {
		t.Error("processTask() with panic exceeding max retries should nack with requeue=false")
	}
}
