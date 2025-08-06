package testutil

import (
	"context"
	"errors"
	"testing"

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
