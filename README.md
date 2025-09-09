# Deltask

<div align="center">

**🚀 一个轻量级、高性能的 Go 分布式任务队列**

[![Go Version](https://img.shields.io/github/go-mod/go-version/gaoxin19/deltask)](https://golang.org/)
[![License](https://img.shields.io/github/license/gaoxin19/deltask)](LICENSE)
[![Go Report Card](https://goreportcard.com/badge/github.com/gaoxin19/deltask)](https://goreportcard.com/report/github.com/gaoxin19/deltask)

</div>

## ✨ 特性

- 🎯 **去中心化设计** - Worker-only, 无需额外的管理节点
- 🔌 **插件化架构** - 当前支持 RabbitMQ，通过 `broker.Broker` 接口可轻松扩展其他消息中间件
- 🌐 **跨语言支持** - 只需发送标准 JSON 任务格式，即可实现跨语言、跨项目的任务分发
- ⏰ **延迟任务支持** - 内置延迟任务功能，精确控制任务执行时间
- 🔄 **重试机制** - 可配置的重试策略，支持指数退避算法
- 🎨 **类型安全参数绑定** - 类似 web 框架的 `ctx.Bind` 功能，支持结构体绑定和类型安全的数据访问


## 🏗️ 架构设计

```mermaid
graph LR
    subgraph "Publisher Layer"
        P[Publisher<br/>任务发布者]
    end
    
    subgraph "Message Broker Layer"
        B[Message Broker<br/>消息中间件]
        Q[Queue<br/>队列]
    end
    
    subgraph "Worker Layer"
        W[Worker<br/>任务处理者]
        H[Handler<br/>处理函数]
    end
    
    P -->|发布任务<br/>跨语言JSON格式| B
    B --> Q
    Q --> W
    W --> H
    
    %% Styling
    classDef publisherClass fill:#e1f5fe,stroke:#01579b,stroke-width:2px
    classDef brokerClass fill:#f3e5f5,stroke:#4a148c,stroke-width:2px
    classDef workerClass fill:#e8f5e8,stroke:#1b5e20,stroke-width:2px
    
    class P publisherClass
    class B,Q brokerClass
    class W,H workerClass
```

### 核心组件

- **Client**: 任务发布客户端，负责将任务发送到指定队列
- **Worker**: 任务处理单元，消费队列中的任务并执行注册的处理函数
- **Broker**: 消息中间件抽象层，当前支持 RabbitMQ
- **Task**: 任务定义，包含任务名称、负载数据、执行时间等信息

## 📦 安装

```bash
go get github.com/gaoxin19/deltask
```

## 🚀 快速开始

### 创建任务发布者 (Publisher)

```go
package main

import (
    "context"
    "log"

    "github.com/gaoxin19/deltask"
    "github.com/gaoxin19/deltask/broker/rabbitmq"
)

func main() {
    // 创建 RabbitMQ broker
    config := rabbitmq.Config{
        URL:       "amqp://admin:admin123@localhost:5672/deltask",
        Namespace: "example",
    }
    
    broker, err := rabbitmq.NewBroker(config)
    if err != nil {
        log.Fatalf("Failed to create broker: %v", err)
    }
    defer broker.Close()

    // 创建客户端
    client := deltask.NewClient(broker)

    // 创建并发布任务
    task := deltask.NewTask("send_email", map[string]any{
        "to":      "user@example.com",
        "subject": "Hello from Deltask!",
        "body":    "This is a test message",
    })

    err = client.Publish(context.Background(), task, "email_queue")
    if err != nil {
        log.Printf("Failed to publish task: %v", err)
    } else {
        log.Println("Task published successfully!")
    }
}
```

### 创建任务处理者 (Worker)

```go
package main

import (
    "context"
    "fmt"
    "log"

    "github.com/gaoxin19/deltask"
    "github.com/gaoxin19/deltask/broker/rabbitmq"
)

// 邮件发送请求结构体
type EmailRequest struct {
    To      string `json:"to"`
    Subject string `json:"subject"`
    Body    string `json:"body"`
}

// 邮件发送处理函数 - 使用 ctx.Bind 进行类型安全的参数解析
func handleSendEmail(ctx *deltask.Context) (any, error) {
    var req EmailRequest
    if err := ctx.Bind(&req); err != nil {
        return nil, fmt.Errorf("failed to bind email request: %w", err)
    }
    
    // 模拟发送邮件
    fmt.Printf("Sending email to: %s\n", req.To)
    fmt.Printf("Subject: %s\n", req.Subject)
    fmt.Printf("Body: %s\n", req.Body)
    
    return map[string]any{
        "status":     "sent",
        "message_id": "msg_12345",
    }, nil
}

func main() {
    // 创建 broker
    config := rabbitmq.Config{
        URL:       "amqp://admin:admin123@localhost:5672/deltask",
        Namespace: "example",
    }
    
    broker, err := rabbitmq.NewBroker(config)
    if err != nil {
        log.Fatalf("Failed to create broker: %v", err)
    }
    defer broker.Close()

    // 创建 worker
    worker := deltask.NewWorker(broker, "email_queue", 2) // 2个并发

    // 注册任务处理函数
    worker.Register("send_email", handleSendEmail)

    // 启动 worker
    fmt.Println("Starting worker...")
    if err := worker.Run(context.Background()); err != nil {
        log.Fatalf("Worker failed: %v", err)
    }
}
```

### 运行示例

```bash
# 运行 Worker
go run worker/main.go

# 在另一个终端运行 Publisher
go run publisher/main.go
```

## 📚 高级用法

### 参数绑定 (ctx.Bind)

Deltask 提供了类似 web 框架的 `ctx.Bind` 功能，支持将任务 payload 直接绑定到结构体，实现类型安全的参数解析：

```go
// 定义请求结构体
type UserRegistrationRequest struct {
    Username string `json:"username"`
    Email    string `json:"email"`
    Age      int    `json:"age"`
}

// 使用 ctx.Bind 进行参数绑定
func handleUserRegistration(ctx *deltask.Context) (any, error) {
    var req UserRegistrationRequest
    if err := ctx.Bind(&req); err != nil {
        return nil, fmt.Errorf("invalid request: %w", err)
    }
    
    // 现在可以直接使用结构体字段，享受类型安全
    fmt.Printf("Registering user: %s (%s), age: %d\n", 
        req.Username, req.Email, req.Age)
    
    return map[string]any{
        "user_id": "user_123",
        "status":  "registered",
    }, nil
}

// 注册处理函数
worker.Register("user_registration", handleUserRegistration)
```

#### 支持的绑定方式

1. **结构体绑定** - 最常用的方式，支持 JSON 标签
2. **基础类型访问** - 通过 `ctx.Get*` 方法获取特定类型的值
3. **原始数据访问** - 通过 `ctx.Payload()` 获取原始 map 数据

```go
func handleFlexibleTask(ctx *deltask.Context) (any, error) {
    // 方式1: 结构体绑定（推荐）
    var req struct {
        Name string `json:"name"`
        ID   int    `json:"id"`
    }
    if err := ctx.Bind(&req); err != nil {
        return nil, err
    }
    
    // 方式2: 类型安全的基础类型访问
    if name, ok := ctx.GetString("name"); ok {
        fmt.Printf("Name: %s\n", name)
    }
    
    if id, ok := ctx.GetInt("id"); ok {
        fmt.Printf("ID: %d\n", id)
    }
    
    // 方式3: 原始数据访问（向后兼容）
    payload := ctx.Payload()
    if value, exists := payload["custom_field"]; exists {
        fmt.Printf("Custom field: %v\n", value)
    }
    
    return "success", nil
}
```

### 延迟任务

```go
// 创建一个 5 分钟后执行的任务
task := deltask.NewTask("cleanup_temp_files", map[string]any{
    "directory": "/tmp/uploads",
})
task.ExecuteAt = time.Now().Add(5 * time.Minute)

client.Publish(ctx, task, "maintenance_queue")
```

### 多队列 Worker

```go
// 定义不同类型的请求结构体
type ImageProcessRequest struct {
    ImageURL string `json:"image_url"`
    Width    int    `json:"width"`
    Height   int    `json:"height"`
    Format   string `json:"format"`
}

type CleanupRequest struct {
    Directory string `json:"directory"`
    MaxAge    int    `json:"max_age_hours"`
}

// 图像处理处理函数
func handleProcessImage(ctx *deltask.Context) (any, error) {
    var req ImageProcessRequest
    if err := ctx.Bind(&req); err != nil {
        return nil, fmt.Errorf("invalid image request: %w", err)
    }
    
    log.Printf("Processing image: %s (%dx%d) to %s", 
        req.ImageURL, req.Width, req.Height, req.Format)
    
    return map[string]any{
        "processed_url": "https://example.com/processed.jpg",
        "size":          "2.5MB",
    }, nil
}

// 清理任务处理函数
func handleCleanupTempFiles(ctx *deltask.Context) (any, error) {
    var req CleanupRequest
    if err := ctx.Bind(&req); err != nil {
        return nil, fmt.Errorf("invalid cleanup request: %w", err)
    }
    
    log.Printf("Cleaning up directory: %s (max age: %d hours)", 
        req.Directory, req.MaxAge)
    
    return map[string]any{
        "files_deleted": 42,
        "space_freed":    "1.2GB",
    }, nil
}

// 创建处理不同类型任务的多个 Worker
workers := map[string]*deltask.Worker{
    "email":       deltask.NewWorker(broker, "email_queue", 2),
    "image":       deltask.NewWorker(broker, "image_queue", 3),
    "maintenance": deltask.NewWorker(broker, "maintenance_queue", 1),
}

// 注册各自的处理函数
workers["email"].Register("send_email", handleSendEmail)
workers["image"].Register("process_image", handleProcessImage)
workers["maintenance"].Register("cleanup_temp_files", handleCleanupTempFiles)

// 并发启动所有 Workers
for name, worker := range workers {
    go func(n string, w *deltask.Worker) {
        log.Printf("Starting %s worker", n)
        if err := w.Run(ctx); err != nil {
            log.Printf("Worker %s failed: %v", n, err)
        }
    }(name, worker)
}
```

### Context 使用示例

Deltask 提供了强大的 Context 功能，支持多种数据访问方式：

```go
// 定义请求结构体
type UserData struct {
    ID       int    `json:"id"`
    Name     string `json:"name"`
    Email    string `json:"email"`
    Age      int    `json:"age"`
    IsActive bool   `json:"is_active"`
}

// 使用 ctx.Bind 进行结构体绑定（推荐方式）
func processUserHandler(ctx *deltask.Context) (any, error) {
    var userData UserData
    if err := ctx.Bind(&userData); err != nil {
        return nil, fmt.Errorf("failed to bind user data: %w", err)
    }
    
    log.Printf("Processing user: ID=%d, Name=%s, Email=%s, Age=%d, Active=%t",
        userData.ID, userData.Name, userData.Email, userData.Age, userData.IsActive)
    
    // 获取任务信息
    taskInfo := ctx.Task()
    log.Printf("Task info: ID=%s, Name=%s, Retry=%d", 
        taskInfo.ID, taskInfo.Name, taskInfo.Retry)
    
    return map[string]any{
        "processed_user_id": userData.ID,
        "status":            "success",
    }, nil
}

// 混合数据访问方式
func flexibleHandler(ctx *deltask.Context) (any, error) {
    // 方式1: 结构体绑定
    type PartialData struct {
        Name string `json:"name"`
        Type string `json:"type"`
    }
    
    var partial PartialData
    if err := ctx.Bind(&partial); err != nil {
        return nil, err
    }
    
    // 方式2: 类型安全的基础类型访问
    count, _ := ctx.GetInt("count")
    score, _ := ctx.GetFloat64("score")
    enabled, _ := ctx.GetBool("enabled")
    
    // 方式3: 原始数据访问（向后兼容）
    rawPayload := ctx.Payload()
    if customField, exists := rawPayload["custom_field"]; exists {
        log.Printf("Custom field: %v", customField)
    }
    
    return map[string]any{
        "processed_name": partial.Name,
        "total_count":    count * 2,
        "final_score":    score + 10,
    }, nil
}
```

## 🌐 跨语言使用

Deltask 支持跨语言任务发布。任何能发送 JSON 消息到 RabbitMQ 的语言都可以发布任务：

### Python 发布者示例

```python
import json
import pika
from datetime import datetime, timezone

# 连接 RabbitMQ
connection = pika.BlockingConnection(
    pika.URLParameters('amqp://admin:admin123@localhost:5672/deltask')
)
channel = connection.channel()

# 创建任务
task = {
    "id": "task-001",
    "name": "send_email",
    "payload": {
        "to": "user@example.com",
        "subject": "Hello from Python!",
        "body": "This task was sent from Python"
    },
    "retry": 0,
    "execute_at": datetime.now(timezone.utc).isoformat()
}

# 发布任务
channel.basic_publish(
    exchange='example.delayed',
    routing_key='email_queue',
    body=json.dumps(task),
    properties=pika.BasicProperties(headers={'x-delay': 0})
)

print("Task published from Python!")
connection.close()
```


## 🔧 配置选项

### RabbitMQ 配置

```go
config := rabbitmq.Config{
    URL:       "amqp://user:pass@host:port/vhost",
    Namespace: "your_namespace", // 用于资源隔离
}
```

### Worker 配置

```go
// 基础配置
worker := deltask.NewWorker(broker, queueName, concurrency)

// 带日志的配置
worker := deltask.NewWorkerWithLogger(broker, queueName, concurrency, logger)
```

### 日志配置

```go
// 生产环境配置
logger := logger.NewProductionLogger()

// 开发环境配置
logger := logger.WithDevelopment()

// 自定义配置
logger, err := logger.NewLogger(logger.Config{
    Level:       "info",     // debug, info, warn, error
    Development: false,      // 开发模式
    Encoding:    "json",     // json, console
})
```

## 🧪 测试

项目提供了完整的测试套件，包含单元测试和集成测试：

```bash
# 运行所有测试
go test ./...

# 运行测试并显示覆盖率
go test -cover ./...

# 生成覆盖率报告
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out
```

### 测试覆盖率

| 模块 | 覆盖率 | 状态 |
|------|--------|------|
| `task` | 100.0% | ✅ 完整覆盖 |
| `logger` | 100.0% | ✅ 完整覆盖 |
| `deltask` | 100.0% | ✅ 完整覆盖 |
| `internal` | 90.5% | ✅ 高覆盖率 |
| `broker/rabbitmq` | 31.5% | ⚠️ 待提升 |


## 🔌 扩展 Broker

Deltask 支持通过实现 `broker.Broker` 接口来扩展其他消息中间件：

```go
type Broker interface {
    io.Closer
    Publish(ctx context.Context, t *task.Task, queueName string) error
    Consume(ctx context.Context, queueName string) (<-chan *task.Task, error)
    Ack(ctx context.Context, t *task.Task) error
    Nack(ctx context.Context, t *task.Task, requeue bool) error
}
```

## 🤝 贡献

欢迎贡献代码！请遵循以下步骤：

1. Fork 项目
2. 创建特性分支 (`git checkout -b feature/AmazingFeature`)
3. 提交更改 (`git commit -m 'Add some AmazingFeature'`)
4. 推送到分支 (`git push origin feature/AmazingFeature`)
5. 开启 Pull Request

### 开发指南

1. 确保代码通过所有测试
2. 添加适当的测试用例
3. 更新相关文档
4. 遵循 Go 代码规范

## 📄 许可证

本项目采用 MIT 许可证。详见 [LICENSE](LICENSE) 文件。

## 🆘 支持

- 🐛 [问题报告](https://github.com/gaoxin19/deltask/issues)

---

<div align="center">

**感谢使用 Deltask！⭐ 如果觉得有用，请给我一个 Star**

</div>