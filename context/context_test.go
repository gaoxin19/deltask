package context

import (
	"context"
	"encoding/json"
	"testing"
)

func TestNew(t *testing.T) {
	ctx := context.Background()
	payload := map[string]any{
		"name":  "test",
		"count": 42,
	}
	payloadBytes, _ := json.Marshal(payload)
	taskInfo := &TaskInfo{
		ID:    "task-123",
		Name:  "test-task",
		Retry: 1,
	}

	c := New(ctx, payloadBytes, taskInfo)

	if c.Context() != ctx {
		t.Error("Context() should return the original context")
	}

	if c.Payload()["name"] != "test" {
		t.Error("Payload() should return the correct deserialized payload")
	}

	if c.Task().ID != "task-123" {
		t.Error("Task() should return the task info")
	}
}

func TestBind(t *testing.T) {
	ctx := context.Background()
	payload := map[string]any{
		"name":   "John Doe",
		"age":    30,
		"email":  "john@example.com",
		"active": true,
		"score":  98.5,
		"tags":   []string{"developer", "golang"},
	}
	payloadBytes, _ := json.Marshal(payload)
	taskInfo := &TaskInfo{ID: "1", Name: "test", Retry: 0}

	c := New(ctx, payloadBytes, taskInfo)

	t.Run("bind to struct", func(t *testing.T) {
		type User struct {
			Name   string   `json:"name"`
			Age    int      `json:"age"`
			Email  string   `json:"email"`
			Active bool     `json:"active"`
			Score  float64  `json:"score"`
			Tags   []string `json:"tags"`
		}

		var user User
		if err := c.Bind(&user); err != nil {
			t.Fatalf("Bind failed: %v", err)
		}

		if user.Name != "John Doe" {
			t.Errorf("Expected name 'John Doe', got '%s'", user.Name)
		}
		if user.Age != 30 {
			t.Errorf("Expected age 30, got %d", user.Age)
		}
		if user.Email != "john@example.com" {
			t.Errorf("Expected email 'john@example.com', got '%s'", user.Email)
		}
		if !user.Active {
			t.Error("Expected active to be true")
		}
		if user.Score != 98.5 {
			t.Errorf("Expected score 98.5, got %f", user.Score)
		}
		if len(user.Tags) != 2 || user.Tags[0] != "developer" {
			t.Errorf("Expected tags ['developer', 'golang'], got %v", user.Tags)
		}
	})

	t.Run("bind to nil should fail", func(t *testing.T) {
		err := c.Bind(nil)
		if err == nil {
			t.Error("Bind to nil should return error")
		}
	})

	t.Run("bind to non-pointer should fail", func(t *testing.T) {
		type User struct {
			Name string `json:"name"`
		}
		var user User
		err := c.Bind(user) // 不是指针
		if err == nil {
			t.Error("Bind to non-pointer should return error")
		}
	})

	t.Run("bind to pointer to non-struct should fail", func(t *testing.T) {
		var str string
		err := c.Bind(&str)
		if err == nil {
			t.Error("Bind to pointer to non-struct should return error")
		}
	})

	t.Run("partial binding", func(t *testing.T) {
		type PartialUser struct {
			Name string `json:"name"`
			Age  int    `json:"age"`
			// 其他字段会被忽略
		}

		var user PartialUser
		if err := c.Bind(&user); err != nil {
			t.Fatalf("Partial bind failed: %v", err)
		}

		if user.Name != "John Doe" {
			t.Errorf("Expected name 'John Doe', got '%s'", user.Name)
		}
		if user.Age != 30 {
			t.Errorf("Expected age 30, got %d", user.Age)
		}
	})
}

func TestGetMethods(t *testing.T) {
	ctx := context.Background()
	payload := map[string]any{
		"string_val": "hello",
		"int_val":    42,
		"int64_val":  int64(123),
		"float_val":  3.14,
		"bool_val":   true,
		"nil_val":    nil,
	}
	payloadBytes, _ := json.Marshal(payload)
	taskInfo := &TaskInfo{ID: "1", Name: "test", Retry: 0}

	c := New(ctx, payloadBytes, taskInfo)

	t.Run("Get", func(t *testing.T) {
		value, exists := c.Get("string_val")
		if !exists {
			t.Error("Expected string_val to exist")
		}
		if value != "hello" {
			t.Errorf("Expected 'hello', got %v", value)
		}

		_, exists = c.Get("nonexistent")
		if exists {
			t.Error("Expected nonexistent key to not exist")
		}
	})

	t.Run("GetString", func(t *testing.T) {
		value, ok := c.GetString("string_val")
		if !ok {
			t.Error("Expected string_val to be retrievable as string")
		}
		if value != "hello" {
			t.Errorf("Expected 'hello', got '%s'", value)
		}

		_, ok = c.GetString("int_val")
		if ok {
			t.Error("Expected int_val to not be retrievable as string")
		}

		_, ok = c.GetString("nonexistent")
		if ok {
			t.Error("Expected nonexistent key to not be retrievable")
		}
	})

	t.Run("GetInt", func(t *testing.T) {
		value, ok := c.GetInt("int_val")
		if !ok {
			t.Error("Expected int_val to be retrievable as int")
		}
		if value != 42 {
			t.Errorf("Expected 42, got %d", value)
		}

		value, ok = c.GetInt("int64_val")
		if !ok {
			t.Error("Expected int64_val to be retrievable as int")
		}
		if value != 123 {
			t.Errorf("Expected 123, got %d", value)
		}

		value, ok = c.GetInt("float_val")
		if !ok {
			t.Error("Expected float_val to be retrievable as int")
		}
		if value != 3 {
			t.Errorf("Expected 3, got %d", value)
		}

		_, ok = c.GetInt("string_val")
		if ok {
			t.Error("Expected string_val to not be retrievable as int")
		}
	})

	t.Run("GetFloat64", func(t *testing.T) {
		value, ok := c.GetFloat64("float_val")
		if !ok {
			t.Error("Expected float_val to be retrievable as float64")
		}
		if value != 3.14 {
			t.Errorf("Expected 3.14, got %f", value)
		}

		value, ok = c.GetFloat64("int_val")
		if !ok {
			t.Error("Expected int_val to be retrievable as float64")
		}
		if value != 42.0 {
			t.Errorf("Expected 42.0, got %f", value)
		}

		_, ok = c.GetFloat64("string_val")
		if ok {
			t.Error("Expected string_val to not be retrievable as float64")
		}
	})

	t.Run("GetBool", func(t *testing.T) {
		value, ok := c.GetBool("bool_val")
		if !ok {
			t.Error("Expected bool_val to be retrievable as bool")
		}
		if !value {
			t.Error("Expected true, got false")
		}

		_, ok = c.GetBool("string_val")
		if ok {
			t.Error("Expected string_val to not be retrievable as bool")
		}
	})
}

func TestSet(t *testing.T) {
	ctx := context.Background()
	payload := map[string]any{"existing": "value"}
	payloadBytes, _ := json.Marshal(payload)
	taskInfo := &TaskInfo{ID: "1", Name: "test", Retry: 0}

	c := New(ctx, payloadBytes, taskInfo)

	c.Set("new_key", "new_value")

	value, exists := c.Get("new_key")
	if !exists {
		t.Error("Expected new_key to exist after Set")
	}
	if value != "new_value" {
		t.Errorf("Expected 'new_value', got %v", value)
	}

	// Test with empty payload
	c2 := New(ctx, []byte("{}"), taskInfo)
	c2.Set("key", "value")

	value, exists = c2.Get("key")
	if !exists {
		t.Error("Expected key to exist after Set on nil payload")
	}
	if value != "value" {
		t.Errorf("Expected 'value', got %v", value)
	}
}
