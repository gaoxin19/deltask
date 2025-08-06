package logger

import (
	"testing"

	"go.uber.org/zap"
)

func TestDefaultConfig(t *testing.T) {
	config := DefaultConfig()

	if config.Level != "info" {
		t.Errorf("DefaultConfig() Level = %v, want 'info'", config.Level)
	}
	if config.Development != false {
		t.Errorf("DefaultConfig() Development = %v, want false", config.Development)
	}
	if config.Encoding != "console" {
		t.Errorf("DefaultConfig() Encoding = %v, want 'console'", config.Encoding)
	}
}

func TestNewLogger(t *testing.T) {
	tests := []struct {
		name    string
		config  Config
		wantErr bool
	}{
		{
			name: "valid production config",
			config: Config{
				Level:       "info",
				Development: false,
				Encoding:    "json",
			},
			wantErr: false,
		},
		{
			name: "valid development config",
			config: Config{
				Level:       "debug",
				Development: true,
				Encoding:    "console",
			},
			wantErr: false,
		},
		{
			name: "invalid level",
			config: Config{
				Level:       "invalid",
				Development: false,
				Encoding:    "json",
			},
			wantErr: true,
		},
		{
			name: "empty encoding should work",
			config: Config{
				Level:       "warn",
				Development: false,
				Encoding:    "",
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, err := NewLogger(tt.config)

			if tt.wantErr {
				if err == nil {
					t.Errorf("NewLogger() error = nil, wantErr %v", tt.wantErr)
				}
				return
			}

			if err != nil {
				t.Errorf("NewLogger() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if logger == nil {
				t.Error("NewLogger() returned nil logger")
			}
		})
	}
}

func TestNewDevelopmentLogger(t *testing.T) {
	logger := NewDevelopmentLogger()

	if logger == nil {
		t.Error("NewDevelopmentLogger() returned nil")
	}

	// Verify it's usable
	logger.Info("test message")
}

func TestNewProductionLogger(t *testing.T) {
	logger := NewProductionLogger()

	if logger == nil {
		t.Error("NewProductionLogger() returned nil")
	}

	// Verify it's usable
	logger.Info("test message")
}

func TestNewNopLogger(t *testing.T) {
	logger := NewNopLogger()

	if logger == nil {
		t.Error("NewNopLogger() returned nil")
	}

	// Verify it's usable and doesn't panic
	logger.Info("test message")
	logger.Error("test error")
}

func TestWithLevel(t *testing.T) {
	tests := []struct {
		name    string
		level   string
		wantErr bool
	}{
		{"debug level", "debug", false},
		{"info level", "info", false},
		{"warn level", "warn", false},
		{"error level", "error", false},
		{"invalid level", "invalid", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, err := WithLevel(tt.level)

			if tt.wantErr {
				if err == nil {
					t.Errorf("WithLevel() error = nil, wantErr %v", tt.wantErr)
				}
				return
			}

			if err != nil {
				t.Errorf("WithLevel() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if logger == nil {
				t.Error("WithLevel() returned nil logger")
			}
		})
	}
}

func TestWithDevelopment(t *testing.T) {
	logger := WithDevelopment()

	if logger == nil {
		t.Error("WithDevelopment() returned nil")
	}

	// Verify it's usable
	logger.Info("test message")
}

func TestConfigValidation(t *testing.T) {
	// Test that config values are properly applied
	config := Config{
		Level:       "error",
		Development: true,
		Encoding:    "json",
	}

	logger, err := NewLogger(config)
	if err != nil {
		t.Fatalf("NewLogger() error = %v", err)
	}

	// We can't easily test the internal configuration of zap logger,
	// but we can verify it was created successfully and is usable
	if logger == nil {
		t.Error("NewLogger() returned nil")
	}

	// Test that it doesn't panic with various log levels
	logger.Debug("debug message")
	logger.Info("info message")
	logger.Warn("warn message")
	logger.Error("error message")
}

func TestLoggerTypeAlias(t *testing.T) {
	// Test that Logger is properly aliased to zap.Logger
	logger := zap.NewNop()

	if logger == nil {
		t.Error("Logger type alias not working correctly")
	}

	// Verify methods are available
	logger.Info("test")
	logger.Error("test")

	// Test type compatibility - Logger is an alias, so this should work
	typedLogger := (*Logger)(logger)
	if typedLogger == nil {
		t.Error("Logger type alias assignment failed")
	}
	typedLogger.Info("test alias")
}
