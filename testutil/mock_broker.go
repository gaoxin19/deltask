// Package testutil 提供测试工具和Mock实现
package testutil

import (
	"context"
	"errors"
	"sync"

	"github.com/gaoxin19/deltask/broker"
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

	// 用于模拟连接断开和重连场景
	mu                 sync.Mutex
	isConnected        bool
	consumeChannels    []chan *task.Task // 保存所有创建的消费通道，用于测试
	simulateDisconnect bool              // 是否模拟连接断开
	disconnectAfter    int               // 在多少次 Consume 调用后断开连接
	consumeCallCount   int               // Consume 调用计数
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
	return &MockBroker{
		isConnected: true,
	}
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

func (m *MockBroker) Consume(ctx context.Context, queueName string, opts ...broker.ConsumeOption) (<-chan *task.Task, error) {
	m.mu.Lock()
	m.consumeCallCount++
	callCount := m.consumeCallCount
	shouldDisconnect := m.simulateDisconnect && callCount > m.disconnectAfter
	m.mu.Unlock()

	m.ConsumeCalls = append(m.ConsumeCalls, ConsumeCall{
		QueueName: queueName,
	})

	// 如果设置了自定义函数，使用它
	if m.ConsumeFunc != nil {
		return m.ConsumeFunc(ctx, queueName)
	}

	// 检查是否应该模拟连接断开
	m.mu.Lock()
	if !m.isConnected {
		m.mu.Unlock()
		return nil, errors.New("broker is not connected")
	}
	m.mu.Unlock()

	// 使用缓冲通道以便测试时可以发送任务
	ch := make(chan *task.Task, 10)

	// 如果应该断开连接，立即关闭通道
	if shouldDisconnect {
		m.mu.Lock()
		m.isConnected = false
		m.mu.Unlock()
		close(ch)
		return ch, nil
	}

	// 保存通道引用，用于测试
	m.mu.Lock()
	m.consumeChannels = append(m.consumeChannels, ch)
	m.mu.Unlock()

	// 如果 context 被取消，关闭通道
	go func() {
		<-ctx.Done()
		m.mu.Lock()
		// 从列表中移除已关闭的通道
		for i, savedCh := range m.consumeChannels {
			if savedCh == ch {
				m.consumeChannels = append(m.consumeChannels[:i], m.consumeChannels[i+1:]...)
				break
			}
		}
		m.mu.Unlock()
		// 安全地关闭通道（检查是否已经关闭）
		select {
		case <-ch:
			// 通道已经关闭
		default:
			close(ch)
		}
	}()

	return ch, nil
}

func (m *MockBroker) Ack(ctx context.Context, t *task.Task) error {
	m.mu.Lock()
	isConnected := m.isConnected
	m.mu.Unlock()

	if !isConnected {
		return errors.New("channel/connection is not open")
	}

	m.AckCalls = append(m.AckCalls, AckCall{Task: t})
	if m.AckFunc != nil {
		return m.AckFunc(ctx, t)
	}
	return nil
}

func (m *MockBroker) Nack(ctx context.Context, t *task.Task, requeue bool) error {
	m.mu.Lock()
	isConnected := m.isConnected
	m.mu.Unlock()

	if !isConnected {
		return errors.New("channel/connection is not open")
	}

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
	m.mu.Lock()
	defer m.mu.Unlock()

	m.PublishCalls = nil
	m.ConsumeCalls = nil
	m.AckCalls = nil
	m.NackCalls = nil
	m.CloseCalls = 0
	m.isConnected = true
	m.consumeChannels = nil
	m.simulateDisconnect = false
	m.disconnectAfter = 0
	m.consumeCallCount = 0
}

// SimulateDisconnect 模拟连接断开
// disconnectAfter: 在第几次 Consume 调用后断开连接（从1开始计数）
func (m *MockBroker) SimulateDisconnect(disconnectAfter int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.simulateDisconnect = true
	m.disconnectAfter = disconnectAfter
}

// Reconnect 模拟重新连接
func (m *MockBroker) Reconnect() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.isConnected = true
}

// Disconnect 手动断开连接
func (m *MockBroker) Disconnect() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.isConnected = false
	// 关闭所有活跃的消费通道
	for _, ch := range m.consumeChannels {
		// 安全地关闭通道（检查是否已经关闭）
		select {
		case <-ch:
			// 通道已经关闭
		default:
			close(ch)
		}
	}
	m.consumeChannels = nil
}

// SendTask 向指定的消费通道发送任务（用于测试）
// 如果 channelIndex 为 -1，则向最后一个通道发送
func (m *MockBroker) SendTask(channelIndex int, t *task.Task) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.consumeChannels) == 0 {
		return false
	}

	var ch chan *task.Task
	if channelIndex == -1 || channelIndex >= len(m.consumeChannels) {
		// 使用最后一个通道
		ch = m.consumeChannels[len(m.consumeChannels)-1]
	} else {
		ch = m.consumeChannels[channelIndex]
	}

	select {
	case ch <- t:
		return true
	default:
		return false
	}
}

// SendTaskToLatest 向最新的消费通道发送任务（用于测试）
func (m *MockBroker) SendTaskToLatest(t *task.Task) bool {
	return m.SendTask(-1, t)
}

// GetConsumeChannelCount 获取当前活跃的消费通道数量
func (m *MockBroker) GetConsumeChannelCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.consumeChannels)
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
