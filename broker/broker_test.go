package broker

import (
	"context"
	"errors"
	"testing"

	"github.com/gaoxin19/deltask/task"
	"github.com/gaoxin19/deltask/testutil"
)

type MockBroker = testutil.MockBroker

func NewMockBroker() *MockBroker {
	return testutil.NewMockBroker()
}

func TestMockBroker(t *testing.T) {
	ctx := context.Background()
	broker := NewMockBroker()

	// 测试 Publish
	testTask := task.New("test", map[string]any{"key": "value"})
	err := broker.Publish(ctx, testTask, "test-queue")
	if err != nil {
		t.Errorf("MockBroker.Publish() error = %v", err)
	}

	if len(broker.PublishCalls) != 1 {
		t.Errorf("Expected 1 publish call, got %d", len(broker.PublishCalls))
	}

	if broker.PublishCalls[0].QueueName != "test-queue" {
		t.Errorf("Expected queue name 'test-queue', got %s", broker.PublishCalls[0].QueueName)
	}

	// 测试 Consume
	ch, err := broker.Consume(ctx, "test-queue")
	if err != nil {
		t.Errorf("MockBroker.Consume() error = %v", err)
	}

	if ch == nil {
		t.Error("MockBroker.Consume() returned nil channel")
	}

	if len(broker.ConsumeCalls) != 1 {
		t.Errorf("Expected 1 consume call, got %d", len(broker.ConsumeCalls))
	}

	// 测试 Ack
	err = broker.Ack(ctx, testTask)
	if err != nil {
		t.Errorf("MockBroker.Ack() error = %v", err)
	}

	if len(broker.AckCalls) != 1 {
		t.Errorf("Expected 1 ack call, got %d", len(broker.AckCalls))
	}

	// 测试 Nack
	err = broker.Nack(ctx, testTask, true)
	if err != nil {
		t.Errorf("MockBroker.Nack() error = %v", err)
	}

	if len(broker.NackCalls) != 1 {
		t.Errorf("Expected 1 nack call, got %d", len(broker.NackCalls))
	}

	if !broker.NackCalls[0].Requeue {
		t.Error("Expected requeue to be true")
	}

	// 测试 Close
	err = broker.Close()
	if err != nil {
		t.Errorf("MockBroker.Close() error = %v", err)
	}

	if broker.CloseCalls != 1 {
		t.Errorf("Expected 1 close call, got %d", broker.CloseCalls)
	}
}

func TestMockBrokerWithCustomFunctions(t *testing.T) {
	ctx := context.Background()
	broker := NewMockBroker()

	// 设置自定义行为
	expectedError := errors.New("publish failed")
	broker.PublishFunc = func(ctx context.Context, t *task.Task, queueName string) error {
		return expectedError
	}

	testTask := task.New("test", nil)
	err := broker.Publish(ctx, testTask, "test-queue")

	if err != expectedError {
		t.Errorf("Expected error %v, got %v", expectedError, err)
	}

	// 验证调用仍然被记录
	if len(broker.PublishCalls) != 1 {
		t.Errorf("Expected 1 publish call, got %d", len(broker.PublishCalls))
	}
}

func TestMockBrokerReset(t *testing.T) {
	ctx := context.Background()
	broker := NewMockBroker()

	// 进行一些调用
	testTask := task.New("test", nil)
	broker.Publish(ctx, testTask, "queue")
	broker.Consume(ctx, "queue")
	broker.Ack(ctx, testTask)
	broker.Nack(ctx, testTask, false)
	broker.Close()

	// 验证调用被记录
	if len(broker.PublishCalls) == 0 || len(broker.ConsumeCalls) == 0 ||
		len(broker.AckCalls) == 0 || len(broker.NackCalls) == 0 ||
		broker.CloseCalls == 0 {
		t.Error("Calls were not recorded properly")
	}

	// 重置
	broker.Reset()

	// 验证所有调用记录被清空
	if len(broker.PublishCalls) != 0 || len(broker.ConsumeCalls) != 0 ||
		len(broker.AckCalls) != 0 || len(broker.NackCalls) != 0 ||
		broker.CloseCalls != 0 {
		t.Error("Reset() did not clear all call records")
	}
}
