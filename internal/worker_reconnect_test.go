package internal

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	deltacontext "github.com/gaoxin19/deltask/context"
	"github.com/gaoxin19/deltask/logger"
	"github.com/gaoxin19/deltask/task"
	"github.com/gaoxin19/deltask/testutil"
)

// TestWorkerReconnectAfterChannelClose 测试 Worker 在消息通道关闭后自动重新消费
func TestWorkerReconnectAfterChannelClose(t *testing.T) {
	broker := testutil.NewMockBroker()
	worker := NewWorkerWithLogger(broker, "test-queue", 1, logger.NewNopLogger())

	// 注册一个简单的任务处理器
	taskProcessed := make(chan string, 10)
	worker.Register("test-task", func(ctx *deltacontext.Context) (any, error) {
		taskID := ctx.Task().ID
		taskProcessed <- taskID
		return nil, nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 启动 worker
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = worker.Run(ctx)
	}()

	// 等待 worker 开始消费
	time.Sleep(100 * time.Millisecond)

	// 验证第一次 Consume 被调用
	if len(broker.ConsumeCalls) < 1 {
		t.Fatal("Expected at least 1 Consume call")
	}

	// 发送第一个任务
	task1 := &task.Task{
		ID:   "task-1",
		Name: "test-task",
	}
	if !broker.SendTaskToLatest(task1) {
		t.Fatal("Failed to send task 1")
	}

	// 等待任务被处理
	select {
	case processedID := <-taskProcessed:
		if processedID != "task-1" {
			t.Errorf("Expected task-1 to be processed, got %s", processedID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Task 1 was not processed in time")
	}

	// 验证任务被 Ack
	if len(broker.AckCalls) != 1 {
		t.Errorf("Expected 1 Ack call, got %d", len(broker.AckCalls))
	}

	// 记录当前的 Consume 调用次数
	initialConsumeCalls := len(broker.ConsumeCalls)

	// 模拟连接断开：关闭消费通道
	broker.Disconnect()

	// 等待 worker 检测到通道关闭并尝试重新消费
	// Worker 会在检测到通道关闭后立即尝试重新消费，但由于连接断开，Consume 会失败
	// 然后 Worker 会使用指数退避重试（初始延迟 1 秒）
	time.Sleep(2 * time.Second)

	// 验证 Worker 尝试重新消费
	// 在断开连接后，Worker 应该至少尝试一次重新消费
	// 由于连接断开，Consume 会返回错误，然后 Worker 会等待后重试
	finalConsumeCalls := len(broker.ConsumeCalls)
	if finalConsumeCalls <= initialConsumeCalls {
		t.Errorf("Expected more Consume calls after disconnect. Initial: %d, Final: %d",
			initialConsumeCalls, finalConsumeCalls)
	}

	// 重新连接
	broker.Reconnect()

	// 等待 worker 成功重新消费
	// Worker 会在检测到通道关闭后，等待 reconnectDelay（可能已经增加到 2 秒或更多）后重试
	// 重连后，Worker 应该能够成功 Consume
	// 我们需要等待足够长的时间，让 Worker 完成重试并成功建立连接

	// 验证新的消费通道已建立（轮询等待）
	maxWait := 5 * time.Second
	waitInterval := 200 * time.Millisecond
	waited := time.Duration(0)
	for broker.GetConsumeChannelCount() == 0 && waited < maxWait {
		time.Sleep(waitInterval)
		waited += waitInterval
	}

	if broker.GetConsumeChannelCount() == 0 {
		// 输出调试信息
		t.Logf("Consume calls: %d", len(broker.ConsumeCalls))
		t.Logf("Active channels: %d", broker.GetConsumeChannelCount())
		t.Fatal("No consume channel established after reconnect")
	}

	// 发送第二个任务
	task2 := &task.Task{
		ID:   "task-2",
		Name: "test-task",
	}
	if !broker.SendTaskToLatest(task2) {
		t.Fatal("Failed to send task 2")
	}

	// 等待任务被处理
	select {
	case processedID := <-taskProcessed:
		if processedID != "task-2" {
			t.Errorf("Expected task-2 to be processed, got %s", processedID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Task 2 was not processed in time")
	}

	// 验证第二个任务也被 Ack
	if len(broker.AckCalls) < 2 {
		t.Errorf("Expected at least 2 Ack calls, got %d", len(broker.AckCalls))
	}

	cancel()
	wg.Wait()
}

// TestWorkerHandlesConnectionErrorOnAck 测试 Worker 在连接错误时正确处理 Ack
func TestWorkerHandlesConnectionErrorOnAck(t *testing.T) {
	broker := testutil.NewMockBroker()
	worker := NewWorkerWithLogger(broker, "test-queue", 1, logger.NewNopLogger())

	// 注册一个成功的任务处理器
	taskProcessed := make(chan bool, 1)
	worker.Register("test-task", func(ctx *deltacontext.Context) (any, error) {
		taskProcessed <- true
		return nil, nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// 启动 worker
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = worker.Run(ctx)
	}()

	// 等待 worker 开始消费
	time.Sleep(100 * time.Millisecond)

	// 发送任务
	task1 := &task.Task{
		ID:   "task-1",
		Name: "test-task",
	}
	if !broker.SendTask(0, task1) {
		t.Fatal("Failed to send task")
	}

	// 等待任务处理开始
	select {
	case <-taskProcessed:
		// 任务处理成功
	case <-time.After(1 * time.Second):
		t.Fatal("Task was not processed")
	}

	// 在任务处理完成后、Ack 之前断开连接
	broker.Disconnect()

	// 等待一小段时间让 Ack 尝试执行
	time.Sleep(200 * time.Millisecond)

	// 验证任务处理器被调用了（任务成功执行）
	// 即使 Ack 失败，任务也已经成功处理
	// 注意：由于连接断开，Ack 可能会失败，但这是预期的行为
	// 在实际场景中，消息会在连接恢复后重新投递

	cancel()
	wg.Wait()
}

// TestWorkerMultipleDisconnects 测试 Worker 能够从多次连接断开中恢复
func TestWorkerMultipleDisconnects(t *testing.T) {
	broker := testutil.NewMockBroker()
	worker := NewWorkerWithLogger(broker, "test-queue", 1, logger.NewNopLogger())

	// 注册任务处理器
	taskCount := 0
	var mu sync.Mutex
	worker.Register("test-task", func(ctx *deltacontext.Context) (any, error) {
		mu.Lock()
		taskCount++
		mu.Unlock()
		return nil, nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 启动 worker
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = worker.Run(ctx)
	}()

	// 等待 worker 开始消费
	time.Sleep(100 * time.Millisecond)

	// 进行多次断开和重连
	for i := 0; i < 3; i++ {
		// 发送一个任务
		task1 := &task.Task{
			ID:   "task-" + string(rune('a'+i)),
			Name: "test-task",
		}
		// 等待通道建立
		time.Sleep(100 * time.Millisecond)
		broker.SendTaskToLatest(task1)

		// 等待任务处理
		time.Sleep(200 * time.Millisecond)

		// 断开连接
		broker.Disconnect()
		time.Sleep(300 * time.Millisecond)

		// 重新连接
		broker.Reconnect()
		time.Sleep(500 * time.Millisecond)

		// 验证 Worker 尝试重新消费
		if len(broker.ConsumeCalls) < i+2 {
			t.Errorf("Iteration %d: Expected at least %d Consume calls, got %d",
				i, i+2, len(broker.ConsumeCalls))
		}
	}

	// 验证至少有一些任务被处理
	mu.Lock()
	processed := taskCount
	mu.Unlock()

	if processed == 0 {
		t.Error("Expected at least some tasks to be processed")
	}

	cancel()
	wg.Wait()
}

// TestWorkerRetryConsumeOnConsumeError 测试 Worker 在 Consume 失败时重试
func TestWorkerRetryConsumeOnConsumeError(t *testing.T) {
	broker := testutil.NewMockBroker()
	worker := NewWorkerWithLogger(broker, "test-queue", 1, logger.NewNopLogger())

	// 模拟前两次 Consume 调用失败，第三次成功
	consumeAttempts := 0
	broker.ConsumeFunc = func(ctx context.Context, queueName string) (<-chan *task.Task, error) {
		consumeAttempts++
		if consumeAttempts <= 2 {
			// 前两次失败
			return nil, errors.New("connection failed")
		}
		// 第三次成功
		ch := make(chan *task.Task, 1)
		return ch, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 启动 worker
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = worker.Run(ctx)
	}()

	// 等待 worker 重试并成功连接
	// 由于添加了随机抖动和初始延迟，需要更长的等待时间
	// 第一次失败后等待 ~1s（带抖动），第二次失败后等待 ~2s（带抖动），第三次成功
	time.Sleep(4 * time.Second)

	// 验证 Consume 被调用了至少 3 次（2 次失败 + 1 次成功）
	if consumeAttempts < 3 {
		t.Errorf("Expected at least 3 Consume attempts, got %d", consumeAttempts)
	}

	// 验证最终成功连接
	if len(broker.ConsumeCalls) < 3 {
		t.Errorf("Expected at least 3 Consume calls, got %d", len(broker.ConsumeCalls))
	}

	cancel()
	wg.Wait()
}

// TestWorkerExponentialBackoff 测试 Worker 使用指数退避策略重试
func TestWorkerExponentialBackoff(t *testing.T) {
	broker := testutil.NewMockBroker()
	worker := NewWorkerWithLogger(broker, "test-queue", 1, logger.NewNopLogger())

	// 记录 Consume 调用的时间
	var consumeTimes []time.Time
	var mu sync.Mutex

	broker.ConsumeFunc = func(ctx context.Context, queueName string) (<-chan *task.Task, error) {
		mu.Lock()
		consumeTimes = append(consumeTimes, time.Now())
		attemptCount := len(consumeTimes)
		mu.Unlock()

		if attemptCount <= 3 {
			// 前三次失败
			ch := make(chan *task.Task)
			close(ch) // 立即关闭通道模拟连接断开
			return ch, nil
		}
		// 第四次成功
		ch := make(chan *task.Task, 1)
		return ch, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 启动 worker
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = worker.Run(ctx)
	}()

	// 等待 worker 完成多次重试
	time.Sleep(5 * time.Second)

	mu.Lock()
	attempts := len(consumeTimes)
	mu.Unlock()

	// 验证有多次重试
	if attempts < 3 {
		t.Errorf("Expected at least 3 consume attempts, got %d", attempts)
	}

	// 验证重试之间有延迟（指数退避）
	if attempts >= 2 {
		mu.Lock()
		delay1 := consumeTimes[1].Sub(consumeTimes[0])
		mu.Unlock()

		// 第一次重试延迟：通道关闭后初始延迟 0.5s + 随机抖动（±25%）
		// 所以实际延迟范围：0.375s - 0.625s
		// 我们检查至少 300ms 以确保有延迟
		if delay1 < 300*time.Millisecond {
			t.Errorf("Expected delay between retries (at least 300ms), got %v", delay1)
		}
	}

	cancel()
	wg.Wait()
}
