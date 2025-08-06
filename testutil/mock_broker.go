// Package testutil 提供测试工具和Mock实现
package testutil

import (
	"context"

	"github.com/gaoxin19/deltask/task"
)

// MockBroker 是用于测试的通用broker实现
// 可以在所有测试包中重复使用
type MockBroker struct {
	PublishCalls []PublishCall
	ConsumeCalls []ConsumeCall
	AckCalls     []AckCall
	NackCalls    []NackCall
	CloseCalls   int

	PublishFunc func(ctx context.Context, t *task.Task, queueName string) error
	ConsumeFunc func(ctx context.Context, queueName string) (<-chan *task.Task, error)
	AckFunc     func(ctx context.Context, t *task.Task) error
	NackFunc    func(ctx context.Context, t *task.Task, requeue bool) error
	CloseFunc   func() error
}

type PublishCall struct {
	Task      *task.Task
	QueueName string
}

type ConsumeCall struct {
	QueueName string
}

type AckCall struct {
	Task *task.Task
}

type NackCall struct {
	Task    *task.Task
	Requeue bool
}

// NewMockBroker 创建一个新的MockBroker实例
func NewMockBroker() *MockBroker {
	return &MockBroker{}
}

func (m *MockBroker) Publish(ctx context.Context, t *task.Task, queueName string) error {
	m.PublishCalls = append(m.PublishCalls, PublishCall{
		Task:      t,
		QueueName: queueName,
	})
	if m.PublishFunc != nil {
		return m.PublishFunc(ctx, t, queueName)
	}
	return nil
}

func (m *MockBroker) Consume(ctx context.Context, queueName string) (<-chan *task.Task, error) {
	m.ConsumeCalls = append(m.ConsumeCalls, ConsumeCall{
		QueueName: queueName,
	})
	if m.ConsumeFunc != nil {
		return m.ConsumeFunc(ctx, queueName)
	}
	ch := make(chan *task.Task)
	close(ch)
	return ch, nil
}

func (m *MockBroker) Ack(ctx context.Context, t *task.Task) error {
	m.AckCalls = append(m.AckCalls, AckCall{Task: t})
	if m.AckFunc != nil {
		return m.AckFunc(ctx, t)
	}
	return nil
}

func (m *MockBroker) Nack(ctx context.Context, t *task.Task, requeue bool) error {
	m.NackCalls = append(m.NackCalls, NackCall{Task: t, Requeue: requeue})
	if m.NackFunc != nil {
		return m.NackFunc(ctx, t, requeue)
	}
	return nil
}

func (m *MockBroker) Close() error {
	m.CloseCalls++
	if m.CloseFunc != nil {
		return m.CloseFunc()
	}
	return nil
}

// Reset 重置所有调用记录，便于在测试之间清理状态
func (m *MockBroker) Reset() {
	m.PublishCalls = nil
	m.ConsumeCalls = nil
	m.AckCalls = nil
	m.NackCalls = nil
	m.CloseCalls = 0
}

// AssertPublishCall 辅助方法，用于验证Publish调用
func (m *MockBroker) AssertPublishCall(index int, expectedTask *task.Task, expectedQueue string) bool {
	if index >= len(m.PublishCalls) {
		return false
	}
	call := m.PublishCalls[index]
	return call.Task == expectedTask && call.QueueName == expectedQueue
}

// AssertPublishCallCount 验证Publish调用次数
func (m *MockBroker) AssertPublishCallCount(expected int) bool {
	return len(m.PublishCalls) == expected
}

// AssertAckCallCount 验证Ack调用次数
func (m *MockBroker) AssertAckCallCount(expected int) bool {
	return len(m.AckCalls) == expected
}

// AssertNackCallCount 验证Nack调用次数
func (m *MockBroker) AssertNackCallCount(expected int) bool {
	return len(m.NackCalls) == expected
}
