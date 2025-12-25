package deltask

import (
	"context"
	"errors"
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

// managerOptions 包含了 Manager 的所有可选配置项
type managerOptions struct {
	logger          *logger.Logger
	shutdownTimeout time.Duration
}

// ManagerOption 定义配置函数
type ManagerOption func(*managerOptions)

// defaultManagerOptions 返回默认配置
func defaultManagerOptions() *managerOptions {
	return &managerOptions{
		logger:          logger.NewProductionLogger(),
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
	mu         sync.Mutex // 保护 errors 切片
	startTime  time.Time
}

// NewManager 创建一个新的 Manager
func NewManager(workers []*Worker, opts ...ManagerOption) *Manager {
	cfg := defaultManagerOptions()
	for _, opt := range opts {
		opt(cfg)
	}

	return &Manager{
		workers:         workers,
		logger:          cfg.logger,
		shutdownTimeout: cfg.shutdownTimeout,
		// 缓冲通道大小设为 worker 数量+1，防止任何情况下的发送阻塞
		errChan:    make(chan error, len(workers)+1),
		signalChan: make(chan os.Signal, 1),
	}
}

// Run 启动并管理所有 Worker，直到收到停止信号或发生致命错误。
// 该方法是阻塞的。
func (m *Manager) Run(ctx context.Context) error {
	if len(m.workers) == 0 {
		return fmt.Errorf("no workers provided to run")
	}

	m.startTime = time.Now()

	// 创建 Manager 专用的上下文，用于控制所有子 Worker
	managerCtx, cancel := context.WithCancel(ctx)
	m.cancel = cancel
	// 确保停止信号监听器被清理
	defer m.stopSignalHandling()

	// 监听系统信号
	signal.Notify(m.signalChan, syscall.SIGINT, syscall.SIGTERM)

	// 启动所有 Worker
	if err := m.startWorkers(managerCtx); err != nil {
		cancel() // 启动失败，取消所有已启动的
		return err
	}

	// 阻塞等待停止条件
	shutdownReason, isError := m.waitForShutdown(managerCtx)

	// 执行优雅关闭
	shutdownErr := m.gracefulShutdown(shutdownReason)

	// 逻辑判断：
	// 如果是 isError (Worker 崩溃)，需要返回错误。
	// 如果 shutdownErr != nil (关闭超时)，需要返回错误。
	// 如果是正常信号退出或 Context Cancel，且关闭过程顺利，返回 nil。
	if !isError && shutdownErr == nil {
		return nil
	}

	return m.combineErrors(shutdownErr)
}

// startWorkers 并发启动所有 Worker
func (m *Manager) startWorkers(ctx context.Context) error {
	queueNames := m.getQueueNames()
	m.logger.Info("Starting deltask worker pool",
		zap.Int("worker_count", len(m.workers)),
		zap.Strings("queues", queueNames),
		zap.Duration("shutdown_timeout", m.shutdownTimeout))

	for i, w := range m.workers {
		m.wg.Add(1)
		go func(id int, worker *Worker) {
			defer m.wg.Done()
			// Worker.Run 现在是阻塞的，直到 Context 取消或发生致命错误
			if err := worker.Run(ctx); err != nil {
				// 忽略 Context Canceled 错误，这是正常的退出信号
				if !errors.Is(err, context.Canceled) {
					m.reportWorkerError(id, worker, err)
				}
			}
		}(i+1, w)
	}

	m.logger.Info("Worker pool started successfully",
		zap.Duration("startup_duration", time.Since(m.startTime)))

	return nil
}

// waitForShutdown 阻塞等待关闭信号
// 返回: (关闭原因, 是否因为错误导致的关闭)
func (m *Manager) waitForShutdown(ctx context.Context) (string, bool) {
	select {
	case sig := <-m.signalChan:
		m.logger.Info("Received shutdown signal", zap.String("signal", sig.String()))
		return fmt.Sprintf("signal %s", sig), false

	case err := <-m.errChan:
		m.appendError(err)
		m.logger.Error("Worker fatal error triggering shutdown", zap.Error(err))
		return "worker error", true

	case <-ctx.Done():
		m.logger.Info("Manager context cancelled")
		return "context cancelled", false
	}
}

// gracefulShutdown 执行优雅关闭流程
func (m *Manager) gracefulShutdown(reason string) error {
	m.logger.Info("Initiating graceful shutdown...", zap.String("reason", reason))

	// 通知所有 Worker 停止接收新任务
	// 由于 Worker 实现是监听 Context Done 的，这里调用 cancel 会触发 Worker 的退出流程
	if m.cancel != nil {
		m.cancel()
	}

	// 等待 Worker 退出或超时
	done := make(chan struct{})
	go func() {
		m.wg.Wait()
		close(done)
	}()

	var shutdownErr error
	select {
	case <-done:
		m.logger.Info("All workers stopped gracefully")
	case <-time.After(m.shutdownTimeout):
		shutdownErr = fmt.Errorf("graceful shutdown timed out after %v", m.shutdownTimeout)
		m.logger.Error("Shutdown timeout, forcing exit", zap.Error(shutdownErr))
	}

	// 收集关闭过程中可能产生的剩余错误
	close(m.errChan)
	for err := range m.errChan {
		m.appendError(err)
	}

	m.logger.Info("Worker pool shutdown completed",
		zap.Duration("total_runtime", time.Since(m.startTime)))

	return shutdownErr
}

func (m *Manager) reportWorkerError(workerID int, worker *Worker, err error) {
	// 使用 select 确保非阻塞，虽然缓冲够大，但这是一种防御性编程习惯
	select {
	case m.errChan <- fmt.Errorf("worker %d (queue='%s') failed: %w",
		workerID, worker.QueueName(), err):
	default:
		m.logger.Error("Failed to report worker error (channel full)",
			zap.Error(err), zap.Int("worker_id", workerID))
	}
}

func (m *Manager) stopSignalHandling() {
	signal.Stop(m.signalChan)
}

func (m *Manager) getQueueNames() []string {
	names := make([]string, len(m.workers))
	for i, w := range m.workers {
		names[i] = w.QueueName()
	}
	return names
}

func (m *Manager) appendError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.errors = append(m.errors, err)
}

func (m *Manager) combineErrors(shutdownErr error) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var allErrors []string

	// 先添加主要错误
	for _, err := range m.errors {
		allErrors = append(allErrors, err.Error())
	}

	if shutdownErr != nil {
		allErrors = append(allErrors, shutdownErr.Error())
	}

	if len(allErrors) == 0 {
		return nil
	}

	return fmt.Errorf("manager stopped with errors: %s", strings.Join(allErrors, "; "))
}
