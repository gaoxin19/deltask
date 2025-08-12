package context

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
)

// Context 包装了任务执行时的上下文信息，提供参数绑定功能。
type Context struct {
	ctx         context.Context // 标准的 Go context
	payloadData []byte          // 原始 payload JSON 字节数据
	cachedMap   map[string]any  // 缓存的 map 数据（延迟解析）
	task        *TaskInfo       // 任务信息
}

// TaskInfo 包含任务的基本信息
type TaskInfo struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Retry int    `json:"retry"`
}

// New 创建一个新的 Context 实例
func New(ctx context.Context, payloadData []byte, taskInfo *TaskInfo) *Context {
	return &Context{
		ctx:         ctx,
		payloadData: payloadData,
		task:        taskInfo,
	}
}

// Context 返回标准的 Go context.Context
func (c *Context) Context() context.Context {
	return c.ctx
}

// Payload 返回原始的 payload 数据（用于向后兼容）
// 这个方法会延迟解析 JSON 数据到 map
func (c *Context) Payload() map[string]any {
	if c.cachedMap == nil {
		c.cachedMap = make(map[string]any)
		if len(c.payloadData) > 0 {
			// 忽略错误，保持向后兼容性
			_ = json.Unmarshal(c.payloadData, &c.cachedMap)
		}
	}
	return c.cachedMap
}

// Task 返回任务信息
func (c *Context) Task() *TaskInfo {
	return c.task
}

// Bind 将 payload 数据绑定到指定的结构体指针
// dst 必须是一个指向结构体的指针
func (c *Context) Bind(dst any) error {
	if dst == nil {
		return fmt.Errorf("bind destination cannot be nil")
	}

	rv := reflect.ValueOf(dst)
	if rv.Kind() != reflect.Ptr {
		return fmt.Errorf("bind destination must be a pointer, got %T", dst)
	}

	rv = rv.Elem()
	if rv.Kind() != reflect.Struct {
		return fmt.Errorf("bind destination must be a pointer to struct, got pointer to %s", rv.Kind())
	}

	// 直接从原始 JSON 字节数据反序列化到目标结构体
	if err := json.Unmarshal(c.payloadData, dst); err != nil {
		return fmt.Errorf("failed to unmarshal payload to struct: %w", err)
	}

	return nil
}

// Get 获取 payload 中的特定字段值
func (c *Context) Get(key string) (any, bool) {
	payload := c.Payload()
	value, exists := payload[key]
	return value, exists
}

// GetString 获取 payload 中的字符串值
func (c *Context) GetString(key string) (string, bool) {
	payload := c.Payload()
	value, exists := payload[key]
	if !exists {
		return "", false
	}

	str, ok := value.(string)
	return str, ok
}

// GetInt 获取 payload 中的整数值
func (c *Context) GetInt(key string) (int, bool) {
	payload := c.Payload()
	value, exists := payload[key]
	if !exists {
		return 0, false
	}

	// 处理不同的数字类型
	switch v := value.(type) {
	case int:
		return v, true
	case int64:
		return int(v), true
	case float64:
		return int(v), true
	default:
		return 0, false
	}
}

// GetFloat64 获取 payload 中的浮点数值
func (c *Context) GetFloat64(key string) (float64, bool) {
	payload := c.Payload()
	value, exists := payload[key]
	if !exists {
		return 0, false
	}

	switch v := value.(type) {
	case float64:
		return v, true
	case float32:
		return float64(v), true
	case int:
		return float64(v), true
	case int64:
		return float64(v), true
	default:
		return 0, false
	}
}

// GetBool 获取 payload 中的布尔值
func (c *Context) GetBool(key string) (bool, bool) {
	payload := c.Payload()
	value, exists := payload[key]
	if !exists {
		return false, false
	}

	boolean, ok := value.(bool)
	return boolean, ok
}

// Set 设置 payload 中的值（通常用于测试或中间件）
func (c *Context) Set(key string, value any) {
	payload := c.Payload()
	payload[key] = value

	// 重新序列化到 payloadData 以保持一致性
	if newData, err := json.Marshal(payload); err == nil {
		c.payloadData = newData
	}
}
