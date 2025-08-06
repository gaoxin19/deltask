// Package logger 提供了基于 zap 的日志功能和依赖注入支持
package logger

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Logger 是 zap.Logger 的类型别名，方便使用
type Logger = zap.Logger

// Config 定义日志配置
type Config struct {
	Level       string `json:"level"`       // 日志级别: debug, info, warn, error
	Development bool   `json:"development"` // 是否为开发模式
	Encoding    string `json:"encoding"`    // 编码格式: json, console
}

// DefaultConfig 返回默认的日志配置
func DefaultConfig() Config {
	return Config{
		Level:       "info",
		Development: false,
		Encoding:    "console",
	}
}

// NewLogger 根据配置创建一个新的 logger 实例
func NewLogger(config Config) (*Logger, error) {
	var zapConfig zap.Config

	if config.Development {
		zapConfig = zap.NewDevelopmentConfig()
		zapConfig.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	} else {
		zapConfig = zap.NewProductionConfig()
	}

	// 设置日志级别
	level, err := zapcore.ParseLevel(config.Level)
	if err != nil {
		return nil, err
	}
	zapConfig.Level = zap.NewAtomicLevelAt(level)

	// 设置编码格式
	if config.Encoding != "" {
		zapConfig.Encoding = config.Encoding
	}

	return zapConfig.Build()
}

// NewDevelopmentLogger 创建一个开发环境的 logger
func NewDevelopmentLogger() *Logger {
	logger, _ := zap.NewDevelopment()
	return logger
}

// NewProductionLogger 创建一个生产环境的 logger
func NewProductionLogger() *Logger {
	logger, _ := zap.NewProduction()
	return logger
}

// NewNopLogger 创建一个空操作的 logger，用于测试
func NewNopLogger() *Logger {
	return zap.NewNop()
}

// WithLevel 返回一个新的logger，使用指定的日志级别
func WithLevel(level string) (*Logger, error) {
	config := DefaultConfig()
	config.Level = level
	return NewLogger(config)
}

// WithDevelopment 返回一个开发环境的logger，使用console编码
func WithDevelopment() *Logger {
	config := DefaultConfig()
	config.Development = true
	config.Encoding = "console"
	logger, _ := NewLogger(config)
	return logger
}
