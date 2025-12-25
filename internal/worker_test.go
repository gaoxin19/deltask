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
			retryCount: 10, // Would be > 5s without cap
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

// TestProcessTask_Success 验证任务成功处理后的流程：Handler被调用 -> Broker Ack
func TestProcessTask_Success(t *testing.T) {
	broker := testutil.NewMockBroker()
	worker := NewWorkerWithLogger(broker, "test-queue", 1, logger.NewNopLogger())

	// Register a successful handler
	handlerCalled := false
	worker.Register("test-task", func(ctx *deltacontext.Context) (any, error) {
		handlerCalled = true
		return "success", nil
	})

	testTask := &task.Task{
		ID:   "test-id",
		Name: "test-task",
	}
	ctx := context.Background()

	worker.processTask(ctx, testTask)

	if !handlerCalled {
		t.Error("Handler should have been called")
	}

	// Should ack the successful task
	if len(broker.AckCalls) != 1 {
		t.Errorf("Ack calls = %v, want 1", len(broker.AckCalls))
	}
	if broker.AckCalls[0].Task != testTask {
		t.Error("Acked wrong task")
	}
}

// TestProcessTask_FailureRetry 验证任务失败后的重试流程：Handler失败 -> Publish Retry -> Ack Old
func TestProcessTask_FailureRetry(t *testing.T) {
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

	// Should publish retry task
	if len(broker.PublishCalls) != 1 {
		t.Errorf("Publish calls = %v, want 1", len(broker.PublishCalls))
	}
	publishedTask := broker.PublishCalls[0].Task
	if publishedTask.Retry != 1 {
		t.Errorf("Retry count = %d, want 1", publishedTask.Retry)
	}

	// Should ack original task
	if len(broker.AckCalls) != 1 {
		t.Errorf("Ack calls = %v, want 1", len(broker.AckCalls))
	}
}

// TestProcessTask_FailureMaxRetries 验证超过最大重试次数：Handler失败 -> Nack(false)
func TestProcessTask_FailureMaxRetries(t *testing.T) {
	broker := testutil.NewMockBroker()
	worker := NewWorkerWithLogger(broker, "test-queue", 1, logger.NewNopLogger())
	// 设置最大重试为3
	worker.WithRetryPolicy(RetryPolicy{MaxRetries: 3})

	worker.Register("test-task", func(ctx *deltacontext.Context) (any, error) {
		return nil, errors.New("failed")
	})

	testTask := &task.Task{
		ID:    "test-id",
		Name:  "test-task",
		Retry: 3, // 已达最大重试
	}
	ctx := context.Background()

	worker.processTask(ctx, testTask)

	// 应该直接丢弃 (Nack false)
	if len(broker.PublishCalls) != 0 {
		t.Error("Should not publish retry task")
	}
	if len(broker.NackCalls) != 1 {
		t.Errorf("Nack calls = %v, want 1", len(broker.NackCalls))
	}
	if broker.NackCalls[0].Requeue != false {
		t.Error("Should Nack with requeue=false")
	}
}

// TestProcessTask_UnknownHandler 验证未知任务类型：直接丢弃 Nack(false)
func TestProcessTask_UnknownHandler(t *testing.T) {
	broker := testutil.NewMockBroker()
	worker := NewWorkerWithLogger(broker, "test-queue", 1, logger.NewNopLogger())

	testTask := &task.Task{
		ID:   "test-id",
		Name: "unknown-task",
	}
	ctx := context.Background()

	worker.processTask(ctx, testTask)

	if len(broker.NackCalls) != 1 {
		t.Errorf("Nack calls = %v, want 1", len(broker.NackCalls))
	}
	if broker.NackCalls[0].Requeue != false {
		t.Error("Should Nack with requeue=false for unknown task")
	}
}

// TestHandleTaskFailure_PublishError 验证重试任务发布失败：必须 Nack Requeue=true 以防丢失
func TestHandleTaskFailure_PublishError(t *testing.T) {
	broker := testutil.NewMockBroker()
	// 模拟 Publish 失败
	broker.PublishFunc = func(ctx context.Context, t *task.Task, queueName string) error {
		return errors.New("network error")
	}

	worker := NewWorkerWithLogger(broker, "test-queue", 1, logger.NewNopLogger())

	testTask := &task.Task{
		ID:    "test-id",
		Name:  "test-task",
		Retry: 0,
	}
	ctx := context.Background()
	taskErr := errors.New("original task failed")

	// 直接调用 handleTaskFailure 测试
	worker.handleTaskFailure(ctx, testTask, taskErr)

	if len(broker.NackCalls) != 1 {
		t.Errorf("Nack calls = %v, want 1", len(broker.NackCalls))
	}
	// 核心验证：发布失败时，原任务必须重新入队，否则会丢消息
	if broker.NackCalls[0].Requeue != true {
		t.Error("Should Nack with requeue=TRUE when publish retry fails")
	}
}

// TestProcessTask_Panic 验证 Panic 捕获：Panic 被视为错误，触发重试流程
func TestProcessTask_Panic(t *testing.T) {
	broker := testutil.NewMockBroker()
	worker := NewWorkerWithLogger(broker, "test-queue", 1, logger.NewNopLogger())

	worker.Register("panic-task", func(ctx *deltacontext.Context) (any, error) {
		panic("boom")
	})

	testTask := &task.Task{
		ID:    "test-id",
		Name:  "panic-task",
		Retry: 0,
	}
	ctx := context.Background()

	// 不应导致测试崩溃
	worker.processTask(ctx, testTask)

	// 视为失败，应发布重试
	if len(broker.PublishCalls) != 1 {
		t.Error("Panic should trigger retry publish")
	}
	if len(broker.AckCalls) != 1 {
		t.Error("Original task should be acked after retry scheduled")
	}
}

// TestWorkerRun 验证 Worker 启动、消费和停止的集成流程
func TestWorkerRun(t *testing.T) {
	broker := testutil.NewMockBroker()
	worker := NewWorkerWithLogger(broker, "test-queue", 1, logger.NewNopLogger())

	// 构造一个模拟 channel
	taskChan := make(chan *task.Task)
	broker.ConsumeFunc = func(ctx context.Context, queueName string) (<-chan *task.Task, error) {
		return taskChan, nil
	}

	worker.Register("test-task", func(ctx *deltacontext.Context) (any, error) {
		return "ok", nil
	})

	ctx, cancel := context.WithCancel(context.Background())

	// 在 goroutine 中运行 Worker，因为 Run 是阻塞的
	errChan := make(chan error)
	go func() {
		errChan <- worker.Run(ctx)
	}()

	// 发送任务
	go func() {
		taskChan <- &task.Task{ID: "1", Name: "test-task"}
		// 稍后取消 Context 以停止 Worker
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	// 等待 Worker 结束
	select {
	case err := <-errChan:
		if err != nil {
			t.Errorf("Worker.Run() returned error: %v", err)
		}
	case <-time.After(1 * time.Second):
		t.Error("Worker did not stop in time")
	}

	// 验证任务被消费
	if len(broker.AckCalls) != 1 {
		t.Errorf("Expected 1 Ack, got %d", len(broker.AckCalls))
	}
}

func TestWorkerWithMultipleConcurrency(t *testing.T) {
	broker := testutil.NewMockBroker()
	concurrency := 3
	worker := NewWorkerWithLogger(broker, "test-queue", concurrency, logger.NewNopLogger())

	taskChan := make(chan *task.Task, 10)
	broker.ConsumeFunc = func(ctx context.Context, queueName string) (<-chan *task.Task, error) {
		return taskChan, nil
	}

	var mu sync.Mutex
	count := 0
	worker.Register("test-task", func(ctx *deltacontext.Context) (any, error) {
		mu.Lock()
		count++
		mu.Unlock()
		time.Sleep(10 * time.Millisecond) // 模拟处理耗时
		return "ok", nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	// 预填充任务
	for i := 0; i < 5; i++ {
		taskChan <- &task.Task{ID: fmt.Sprintf("%d", i), Name: "test-task"}
	}

	go func() {
		_ = worker.Run(ctx)
	}()

	// 等待任务处理完成或超时
	time.Sleep(200 * time.Millisecond)
	cancel() // 停止 Worker

	mu.Lock()
	if count != 5 {
		t.Errorf("Expected 5 tasks processed, got %d", count)
	}
	mu.Unlock()
}
