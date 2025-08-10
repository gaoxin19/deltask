package deltask

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gaoxin19/deltask/logger"
	"go.uber.org/zap"
)

// managerOptions 包含了 Manager 的所有可选配置项。
// 这是一个私有结构体，只能通过 ManagerOption 函数来修改。
type managerOptions struct {
	logger          *logger.Logger
	shutdownTimeout time.Duration
}

// ManagerOption 是一个函数类型，用于修改 managerOptions 结构体。
type ManagerOption func(*managerOptions)

// newManagerOptions 创建一个带有默认值的 managerOptions 实例。
func newManagerOptions() *managerOptions {
	return &managerOptions{
		// 默认的 logger
		logger: logger.NewProductionLogger(),
		// 默认的优雅关闭超时时间
		shutdownTimeout: 30 * time.Second,
	}
}

// ManagerOpts 包含 Manager 相关的配置选项。
var ManagerOpts managerOptionBuilder

type managerOptionBuilder struct{}

// WithLogger 返回一个 ManagerOption，用于设置自定义的 logger。
func (managerOptionBuilder) WithLogger(l *logger.Logger) ManagerOption {
	return func(o *managerOptions) {
		o.logger = l
	}
}

// WithShutdownTimeout 返回一个 ManagerOption，用于设置优雅关闭的超时时间。
// 如果在超时时间内 workers 仍未停止，Manager.Run 将返回一个错误。
func (managerOptionBuilder) WithShutdownTimeout(timeout time.Duration) ManagerOption {
	return func(o *managerOptions) {
		o.shutdownTimeout = timeout
	}
}

// Manager 管理多个 Worker 的生命周期
type Manager struct {
	workers         []*Worker
	logger          *logger.Logger
	shutdownTimeout time.Duration

	// 运行时状态
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	errChan    chan error
	signalChan chan os.Signal
	errors     []error
	startTime  time.Time
}

// NewManager 创建一个新的 Manager
func NewManager(workers []*Worker, opts ...ManagerOption) *Manager {
	cfg := newManagerOptions()
	for _, opt := range opts {
		opt(cfg)
	}

	return &Manager{
		workers:         workers,
		logger:          cfg.logger,
		shutdownTimeout: cfg.shutdownTimeout,
		errChan:         make(chan error, len(workers)),
		signalChan:      make(chan os.Signal, 1),
	}
}

// Run 启动并管理所有 Worker，直到收到停止信号
func (m *Manager) Run(ctx context.Context) error {
	if len(m.workers) == 0 {
		return fmt.Errorf("no workers provided to run")
	}

	m.startTime = time.Now()

	// 设置上下文和信号处理
	managerCtx, cancel := context.WithCancel(ctx)
	m.cancel = cancel
	defer cancel()
	defer m.cleanup()

	signal.Notify(m.signalChan, syscall.SIGINT, syscall.SIGTERM)

	// 启动 workers
	if err := m.startWorkers(managerCtx); err != nil {
		return err
	}

	// 等待停止信号
	shutdownReason := m.waitForShutdown(managerCtx)

	// 执行优雅关闭
	return m.gracefulShutdown(shutdownReason)
}

// startWorkers 启动所有 Worker
func (m *Manager) startWorkers(ctx context.Context) error {
	queueNames := m.getQueueNames()
	m.logger.Info("Starting deltask worker pool",
		zap.Int("worker_count", len(m.workers)),
		zap.Strings("queues", queueNames),
		zap.Duration("shutdown_timeout", m.shutdownTimeout))

	for i, w := range m.workers {
		m.wg.Add(1)
		go func(workerID int, worker *Worker) {
			defer m.wg.Done()
			if err := worker.Run(ctx); err != nil {
				m.reportWorkerError(workerID, worker, err)
			}
		}(i+1, w)
	}

	m.logger.Info("Worker pool started successfully",
		zap.Duration("startup_duration", time.Since(m.startTime)))

	return nil
}

// waitForShutdown 等待第一个停止信号
func (m *Manager) waitForShutdown(ctx context.Context) string {
	select {
	case sig := <-m.signalChan:
		reason := fmt.Sprintf("signal %s", sig.String())
		m.logger.Info("Received shutdown signal", zap.String("signal", sig.String()))
		return reason
	case err := <-m.errChan:
		m.errors = append(m.errors, err)
		m.logger.Error("Worker failed", zap.Error(err))
		return "worker error"
	case <-ctx.Done():
		m.errors = append(m.errors, ctx.Err())
		m.logger.Info("Parent context cancelled")
		return "context cancelled"
	}
}

// gracefulShutdown 执行优雅关闭
func (m *Manager) gracefulShutdown(reason string) error {
	// 广播关闭信号
	m.cancel()

	// 收集剩余错误
	go m.collectRemainingErrors()

	// 等待所有 workers 停止
	shutdownComplete := make(chan struct{})
	go func() {
		m.wg.Wait()
		close(m.errChan)
		close(shutdownComplete)
	}()

	select {
	case <-shutdownComplete:
		m.logger.Info("All workers stopped gracefully",
			zap.String("reason", reason),
			zap.Int("error_count", len(m.errors)))
	case <-time.After(m.shutdownTimeout):
		shutdownErr := fmt.Errorf("shutdown timed out after %v", m.shutdownTimeout)
		m.errors = append(m.errors, shutdownErr)
		m.logger.Error("Shutdown timeout", zap.Duration("timeout", m.shutdownTimeout))
	}

	totalDuration := time.Since(m.startTime)
	m.logger.Info("Worker pool shutdown completed",
		zap.Duration("total_runtime", totalDuration),
		zap.String("shutdown_reason", reason))

	return m.combineErrors()
}

// reportWorkerError 报告 Worker 错误
func (m *Manager) reportWorkerError(workerID int, worker *Worker, err error) {
	select {
	case m.errChan <- fmt.Errorf("worker %d (queue='%s') failed: %w",
		workerID, worker.w.QueueName(), err):
	default:
		m.logger.Error("Failed to report worker error",
			zap.Error(err), zap.Int("worker_id", workerID))
	}
}

// collectRemainingErrors 收集剩余的错误
func (m *Manager) collectRemainingErrors() {
	for err := range m.errChan {
		m.errors = append(m.errors, err)
		m.logger.Error("Additional worker error during shutdown", zap.Error(err))
	}
}

// cleanup 清理资源
func (m *Manager) cleanup() {
	signal.Stop(m.signalChan)
}

// getQueueNames 从 workers 中提取队列名称
func (m *Manager) getQueueNames() []string {
	names := make([]string, len(m.workers))
	for i, w := range m.workers {
		names[i] = w.w.QueueName()
	}
	return names
}

// combineErrors 合并多个错误为单个错误
func (m *Manager) combineErrors() error {
	if len(m.errors) == 0 {
		return nil
	}
	if len(m.errors) == 1 {
		return m.errors[0]
	}

	// 组合多个错误
	var errMsgs []string
	for _, err := range m.errors {
		errMsgs = append(errMsgs, err.Error())
	}
	return fmt.Errorf("multiple errors occurred: %s", strings.Join(errMsgs, "; "))
}
