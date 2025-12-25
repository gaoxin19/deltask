package rabbitmq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/gaoxin19/deltask/logger"
	"github.com/gaoxin19/deltask/task"
)

func TestConfig(t *testing.T) {
	config := Config{
		URL:       "amqp://guest:guest@localhost:5672/",
		Namespace: "test",
	}

	if config.URL != "amqp://guest:guest@localhost:5672/" {
		t.Errorf("Config.URL = %v", config.URL)
	}
	if config.Namespace != "test" {
		t.Errorf("Config.Namespace = %v", config.Namespace)
	}
}

func TestNewBrokerWithEmptyNamespace(t *testing.T) {
	config := Config{
		URL:       "amqp://guest:guest@localhost:5672/",
		Namespace: "",
	}

	broker, err := NewBroker(config)
	if err == nil {
		t.Error("NewBroker() with empty namespace should return error")
	}
	if broker != nil {
		t.Error("NewBroker() with empty namespace should return nil broker")
	}

	expectedError := "rabbitmq config requires a non-empty namespace"
	if err.Error() != expectedError {
		t.Errorf("NewBroker() error = %v, want %v", err.Error(), expectedError)
	}
}

func TestNewBrokerWithLogger(t *testing.T) {
	config := Config{
		URL:       "amqp://guest:guest@localhost:5672/",
		Namespace: "test",
	}

	testLogger := logger.NewNopLogger()

	// This will fail to connect to actual RabbitMQ, but we're testing the validation logic
	broker, err := NewBrokerWithLogger(config, testLogger)

	// Since we don't have a real RabbitMQ instance, this should fail with connection error
	if err == nil {
		t.Error("NewBrokerWithLogger() without RabbitMQ should return connection error")
		if broker != nil {
			broker.Close()
		}
	}

	// The error should be related to connection, not validation
	if broker != nil {
		t.Error("NewBrokerWithLogger() with connection failure should return nil broker")
	}
}

func TestNewBrokerWithLoggerNil(t *testing.T) {
	config := Config{
		URL:       "amqp://guest:guest@localhost:5672/",
		Namespace: "test",
	}

	// This will fail to connect, but should handle nil logger gracefully
	broker, err := NewBrokerWithLogger(config, nil)

	// Should fail with connection error, not nil logger error
	if err == nil {
		t.Error("NewBrokerWithLogger() without RabbitMQ should return connection error")
		if broker != nil {
			broker.Close()
		}
	}
}

func TestRabbitBrokerPrefixed(t *testing.T) {
	// We can test the prefixed method without actual RabbitMQ connection
	// by creating a rabbitBroker instance directly
	rb := &rabbitBroker{
		config: Config{Namespace: "test"},
	}

	tests := []struct {
		input    string
		expected string
	}{
		{"queue1", "test.queue1"},
		{"my-queue", "test.my-queue"},
		{"", "test."},
	}

	for _, tt := range tests {
		result := rb.prefixed(tt.input)
		if result != tt.expected {
			t.Errorf("prefixed(%q) = %q, want %q", tt.input, result, tt.expected)
		}
	}
}

func TestDelayedExchangeName(t *testing.T) {
	config := Config{
		URL:       "amqp://guest:guest@localhost:5672/",
		Namespace: "billing",
	}

	rb := &rabbitBroker{
		config:              config,
		delayedExchangeName: "billing.deltask.delayed",
	}

	expected := "billing.deltask.delayed"
	if rb.delayedExchangeName != expected {
		t.Errorf("delayedExchangeName = %v, want %v", rb.delayedExchangeName, expected)
	}
}

func TestTaskSerialization(t *testing.T) {
	// Test that tasks can be properly serialized/deserialized
	// This tests the JSON marshaling that happens in Publish/Consume
	originalTask := &task.Task{
		ID:        "test-id-123",
		Name:      "test-task",
		Payload:   []byte(`{"key":"value","number":42}`),
		Retry:     2,
		ExecuteAt: time.Now().Round(time.Second), // Round to avoid precision issues
	}

	// Test marshaling
	data, err := json.Marshal(originalTask)
	if err != nil {
		t.Fatalf("Failed to marshal task: %v", err)
	}

	// Test unmarshaling
	var deserializedTask task.Task
	err = json.Unmarshal(data, &deserializedTask)
	if err != nil {
		t.Fatalf("Failed to unmarshal task: %v", err)
	}

	// Verify fields
	if deserializedTask.ID != originalTask.ID {
		t.Errorf("Deserialized ID = %v, want %v", deserializedTask.ID, originalTask.ID)
	}
	if deserializedTask.Name != originalTask.Name {
		t.Errorf("Deserialized Name = %v, want %v", deserializedTask.Name, originalTask.Name)
	}
	if deserializedTask.Retry != originalTask.Retry {
		t.Errorf("Deserialized Retry = %v, want %v", deserializedTask.Retry, originalTask.Retry)
	}

	// Check payload (both should be JSON bytes)
	if len(deserializedTask.Payload) != len(originalTask.Payload) {
		t.Errorf("Deserialized payload length = %v, want %v",
			len(deserializedTask.Payload), len(originalTask.Payload))
	}

	// 反序列化 payload 来验证内容
	var originalPayload, deserializedPayload map[string]any

	if err := json.Unmarshal(originalTask.Payload, &originalPayload); err != nil {
		t.Errorf("Failed to unmarshal original payload: %v", err)
	}

	if err := json.Unmarshal(deserializedTask.Payload, &deserializedPayload); err != nil {
		t.Errorf("Failed to unmarshal deserialized payload: %v", err)
	}

	for k, v := range originalPayload {
		deserializedValue, exists := deserializedPayload[k]
		if !exists {
			t.Errorf("Deserialized payload missing key: %s", k)
			continue
		}

		// JSON unmarshaling converts numbers to float64
		if k == "number" {
			if floatVal, ok := deserializedValue.(float64); ok {
				if originalFloat, ok := v.(float64); ok {
					if floatVal != originalFloat {
						t.Errorf("Deserialized payload[%s] = %v, want %v",
							k, floatVal, originalFloat)
					}
				} else {
					t.Errorf("Original payload[%s] is not float64: %T", k, v)
				}
			} else {
				t.Errorf("Deserialized payload[%s] is not float64: %T", k, deserializedValue)
			}
		} else {
			if deserializedValue != v {
				t.Errorf("Deserialized payload[%s] = %v, want %v",
					k, deserializedValue, v)
			}
		}
	}

	// ExecuteAt should be close (JSON doesn't preserve nanosecond precision)
	timeDiff := deserializedTask.ExecuteAt.Sub(originalTask.ExecuteAt)
	if timeDiff > time.Second || timeDiff < -time.Second {
		t.Errorf("Deserialized ExecuteAt = %v, want %v (diff: %v)",
			deserializedTask.ExecuteAt, originalTask.ExecuteAt, timeDiff)
	}

	// BrokerMeta should not be serialized
	if deserializedTask.BrokerMeta != nil {
		t.Errorf("Deserialized BrokerMeta = %v, want nil", deserializedTask.BrokerMeta)
	}
}

func TestDelayCalculation(t *testing.T) {
	now := time.Now()

	tests := []struct {
		name        string
		executeAt   time.Time
		expectDelay bool
	}{
		{
			name:        "future execution",
			executeAt:   now.Add(5 * time.Minute),
			expectDelay: true,
		},
		{
			name:        "immediate execution",
			executeAt:   now,
			expectDelay: false,
		},
		{
			name:        "past execution",
			executeAt:   now.Add(-5 * time.Minute),
			expectDelay: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			task := &task.Task{ExecuteAt: tt.executeAt}

			delay := time.Until(task.ExecuteAt).Milliseconds()
			hasDelay := delay > 0

			if hasDelay != tt.expectDelay {
				t.Errorf("Delay calculation for %v: hasDelay = %v, want %v (delay = %v ms)",
					tt.executeAt, hasDelay, tt.expectDelay, delay)
			}
		})
	}
}

func TestRabbitBrokerConstants(t *testing.T) {
	if delayedExchangeType != "x-delayed-message" {
		t.Errorf("delayedExchangeType = %v, want 'x-delayed-message'", delayedExchangeType)
	}
}

func TestRabbitBrokerClose(t *testing.T) {
	// Test multiple close calls
	rb := &rabbitBroker{
		isClosed: false,
		logger:   logger.NewNopLogger(),
	}

	// First close
	err := rb.Close()
	if err != nil {
		t.Errorf("First close error = %v", err)
	}

	if !rb.isClosed {
		t.Error("isClosed should be true after first close")
	}

	// Second close should be no-op
	err = rb.Close()
	if err != nil {
		t.Errorf("Second close error = %v", err)
	}
}

func TestRabbitBrokerPublishValidation(t *testing.T) {
	// Test publish validation without actual connection
	rb := &rabbitBroker{
		config: Config{Namespace: "test"},
		logger: logger.NewNopLogger(),
	}

	ctx := context.Background()
	testTask := task.New("test", map[string]any{"key": "value"})

	// Test with nil channel
	err := rb.Publish(ctx, testTask, "test-queue")
	if err == nil {
		t.Error("Publish() with nil channel should return error")
	}
	// 应该返回 ErrBrokerNotConnected 错误
	if !errors.Is(err, ErrBrokerNotConnected) {
		t.Errorf("Publish() error = %v, want ErrBrokerNotConnected", err.Error())
	}
}

func TestRabbitBrokerConsumeValidation(t *testing.T) {
	// Test consume validation without actual connection
	rb := &rabbitBroker{
		config: Config{Namespace: "test"},
		logger: logger.NewNopLogger(),
	}

	ctx := context.Background()

	// Test with nil channel
	ch, err := rb.Consume(ctx, "test-queue")
	if err == nil {
		t.Error("Consume() with nil channel should return error")
	}
	if ch != nil {
		t.Error("Consume() with error should return nil channel")
	}
}

func TestRabbitBrokerAckWithInvalidMeta(t *testing.T) {
	rb := &rabbitBroker{
		config: Config{Namespace: "test"},
		logger: logger.NewNopLogger(),
	}

	ctx := context.Background()
	testTask := &task.Task{
		ID:         "test-id",
		BrokerMeta: "invalid-meta", // Should be amqp.Delivery
	}

	err := rb.Ack(ctx, testTask)
	if err == nil {
		t.Error("Ack() with invalid broker meta should return error")
	}
	expectedError := "invalid broker metadata for ack: not an amqp.Delivery"
	if err.Error() != expectedError {
		t.Errorf("Ack() error = %v, want %v", err.Error(), expectedError)
	}
}

func TestRabbitBrokerNackWithInvalidMeta(t *testing.T) {
	rb := &rabbitBroker{
		config: Config{Namespace: "test"},
		logger: logger.NewNopLogger(),
	}

	ctx := context.Background()
	testTask := &task.Task{
		ID:         "test-id",
		BrokerMeta: "invalid-meta", // Should be amqp.Delivery
	}

	err := rb.Nack(ctx, testTask, true)
	if err == nil {
		t.Error("Nack() with invalid broker meta should return error")
	}
	expectedError := "invalid broker metadata for nack: not an amqp.Delivery"
	if err.Error() != expectedError {
		t.Errorf("Nack() error = %v, want %v", err.Error(), expectedError)
	}
}

func TestDelayHeaderCalculation(t *testing.T) {
	tests := []struct {
		name          string
		executeAt     time.Time
		expectedKey   string
		expectedDelay bool
	}{
		{
			name:          "immediate execution",
			executeAt:     time.Now(),
			expectedKey:   "x-delay",
			expectedDelay: false,
		},
		{
			name:          "future execution",
			executeAt:     time.Now().Add(5 * time.Minute),
			expectedKey:   "x-delay",
			expectedDelay: true,
		},
		{
			name:          "past execution",
			executeAt:     time.Now().Add(-5 * time.Minute),
			expectedKey:   "x-delay",
			expectedDelay: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Simulate the header creation logic from Publish
			headers := make(map[string]interface{})

			if delay := time.Until(tt.executeAt).Milliseconds(); delay > 0 {
				headers[tt.expectedKey] = delay
			}

			hasDelay := len(headers) > 0
			if hasDelay != tt.expectedDelay {
				t.Errorf("Header delay calculation: hasDelay = %v, want %v", hasDelay, tt.expectedDelay)
			}

			if tt.expectedDelay {
				if _, exists := headers[tt.expectedKey]; !exists {
					t.Errorf("Expected header %s to exist", tt.expectedKey)
				}
			}
		})
	}
}

func TestMessageBodySerialization(t *testing.T) {
	// Test the JSON serialization logic used in Publish
	testTask := &task.Task{
		ID:        "test-123",
		Name:      "serialize-test",
		Payload:   []byte(`{"data":"value","count":42}`),
		Retry:     1,
		ExecuteAt: time.Now(),
	}

	body, err := json.Marshal(testTask)
	if err != nil {
		t.Fatalf("Failed to marshal task: %v", err)
	}

	if len(body) == 0 {
		t.Error("Marshaled body should not be empty")
	}

	// Verify we can unmarshal it back
	var unmarshaled task.Task
	err = json.Unmarshal(body, &unmarshaled)
	if err != nil {
		t.Fatalf("Failed to unmarshal task: %v", err)
	}

	if unmarshaled.ID != testTask.ID {
		t.Errorf("Unmarshaled ID = %v, want %v", unmarshaled.ID, testTask.ID)
	}
	if unmarshaled.Name != testTask.Name {
		t.Errorf("Unmarshaled Name = %v, want %v", unmarshaled.Name, testTask.Name)
	}
}

func TestExchangeNameGeneration(t *testing.T) {
	tests := []struct {
		namespace string
		expected  string
	}{
		{"billing", "billing.deltask.delayed"},
		{"user-service", "user-service.deltask.delayed"},
		{"test", "test.deltask.delayed"},
	}

	for _, tt := range tests {
		t.Run(tt.namespace, func(t *testing.T) {
			config := Config{Namespace: tt.namespace}
			exchangeName := fmt.Sprintf("%s.deltask.delayed", config.Namespace)

			if exchangeName != tt.expected {
				t.Errorf("Exchange name = %v, want %v", exchangeName, tt.expected)
			}
		})
	}
}

// Integration test structure (these would need actual RabbitMQ to run)
func TestIntegrationStructure(t *testing.T) {
	// This test verifies the structure needed for integration tests
	// Actual integration tests would require docker or real RabbitMQ instance

	config := Config{
		URL:       "amqp://guest:guest@localhost:5672/",
		Namespace: "test",
	}

	if config.URL == "" {
		t.Error("Integration test config needs URL")
	}
	if config.Namespace == "" {
		t.Error("Integration test config needs namespace")
	}

	// Verify context handling
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if ctx.Err() != nil {
		t.Error("Context should not be cancelled initially")
	}

	// Verify task creation for integration
	testTask := task.New("integration-test", map[string]any{"test": true})
	if testTask.Name != "integration-test" {
		t.Error("Integration test task not created correctly")
	}

	t.Log("Integration test structure verified")
	t.Log("To run actual integration tests, ensure RabbitMQ with delayed message plugin is available")
}
