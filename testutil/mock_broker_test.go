package testutil

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/gaoxin19/deltask/task"
)

func TestMockBrokerBasicUsage(t *testing.T) {
	broker := NewMockBroker()
	ctx := context.Background()

	// Test Publish
	testTask := task.New("test", map[string]any{"key": "value"})
	err := broker.Publish(ctx, testTask, "test-queue")
	if err != nil {
		t.Errorf("Publish() error = %v", err)
	}

	// Verify call was recorded
	if !broker.AssertPublishCallCount(1) {
		t.Error("Expected 1 publish call")
	}

	if !broker.AssertPublishCall(0, testTask, "test-queue") {
		t.Error("Publish call not recorded correctly")
	}

	// Test Ack
	err = broker.Ack(ctx, testTask)
	if err != nil {
		t.Errorf("Ack() error = %v", err)
	}

	if !broker.AssertAckCallCount(1) {
		t.Error("Expected 1 ack call")
	}
}

func TestMockBrokerWithCustomFunctions(t *testing.T) {
	broker := NewMockBroker()
	ctx := context.Background()

	// Set custom behavior
	expectedError := errors.New("custom error")
	broker.PublishFunc = func(ctx context.Context, t *task.Task, queueName string) error {
		return expectedError
	}

	testTask := task.New("test", nil)
	err := broker.Publish(ctx, testTask, "test-queue")

	if err != expectedError {
		t.Errorf("Expected custom error, got %v", err)
	}

	// Verify call was still recorded
	if !broker.AssertPublishCallCount(1) {
		t.Error("Call should still be recorded even with custom function")
	}
}

func TestMockBrokerReset(t *testing.T) {
	broker := NewMockBroker()
	ctx := context.Background()

	// Make some calls
	testTask := task.New("test", nil)
	broker.Publish(ctx, testTask, "queue")
	broker.Ack(ctx, testTask)
	broker.Nack(ctx, testTask, false)
	broker.Close()

	// Verify calls were recorded
	if !broker.AssertPublishCallCount(1) {
		t.Error("Expected 1 publish call before reset")
	}
	if !broker.AssertAckCallCount(1) {
		t.Error("Expected 1 ack call before reset")
	}
	if !broker.AssertNackCallCount(1) {
		t.Error("Expected 1 nack call before reset")
	}
	if broker.CloseCalls != 1 {
		t.Error("Expected 1 close call before reset")
	}

	// Reset
	broker.Reset()

	// Verify all counters are reset
	if !broker.AssertPublishCallCount(0) {
		t.Error("Publish calls should be reset")
	}
	if !broker.AssertAckCallCount(0) {
		t.Error("Ack calls should be reset")
	}
	if !broker.AssertNackCallCount(0) {
		t.Error("Nack calls should be reset")
	}
	if broker.CloseCalls != 0 {
		t.Error("Close calls should be reset")
	}
}

func TestMockBrokerDisconnectReconnect(t *testing.T) {
	broker := NewMockBroker()
	ctx := context.Background()

	// 测试正常连接
	ch, err := broker.Consume(ctx, "test-queue")
	if err != nil {
		t.Fatalf("Consume() error = %v", err)
	}

	// 发送任务
	task1 := task.New("test", nil)
	if !broker.SendTaskToLatest(task1) {
		t.Error("Failed to send task")
	}

	// 接收任务
	select {
	case receivedTask := <-ch:
		if receivedTask == nil {
			t.Error("Received nil task")
		}
	case <-time.After(time.Second):
		t.Error("Failed to receive task")
	}

	// 断开连接
	broker.Disconnect()

	// 验证 Ack 失败
	err = broker.Ack(ctx, task1)
	if err == nil {
		t.Error("Expected error on Ack after disconnect")
	}
	if err.Error() != "channel/connection is not open" {
		t.Errorf("Expected 'channel/connection is not open' error, got %v", err)
	}

	// 重新连接
	broker.Reconnect()

	// 验证可以再次消费
	ch2, err := broker.Consume(ctx, "test-queue")
	if err != nil {
		t.Fatalf("Consume() after reconnect error = %v", err)
	}

	// 发送新任务
	task2 := task.New("test2", nil)
	if !broker.SendTaskToLatest(task2) {
		t.Error("Failed to send task after reconnect")
	}

	// 接收新任务
	select {
	case receivedTask := <-ch2:
		if receivedTask == nil {
			t.Error("Received nil task after reconnect")
		}
	case <-time.After(time.Second):
		t.Error("Failed to receive task after reconnect")
	}
}
