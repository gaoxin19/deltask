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


## 🏗️ 架构设计

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Publisher     │    │   Message       │    │     Worker      │
│   (任务发布者)  │    │   Broker        │    │   (任务处理者)  │
│                 │    │                 │    │                 │
│  ┌───────────┐  │    │  ┌───────────┐  │    │  ┌───────────┐  │
│  │  Client   │──┼────┼─▶│  Queue    │──┼────┼─▶│  Handler  │  │
│  └───────────┘  │    │  └───────────┘  │    │  └───────────┘  │
└─────────────────┘    └─────────────────┘    └─────────────────┘
        │                       │                            │
        └──── 发布任务 ──────────────────────────────────────┘
                              (跨语言JSON格式)
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

// 邮件发送处理函数
func handleSendEmail(ctx context.Context, payload map[string]any) (any, error) {
    to := payload["to"].(string)
    subject := payload["subject"].(string)
    body := payload["body"].(string)
    
    // 模拟发送邮件
    fmt.Printf("Sending email to: %s\n", to)
    fmt.Printf("Subject: %s\n", subject)
    fmt.Printf("Body: %s\n", body)
    
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