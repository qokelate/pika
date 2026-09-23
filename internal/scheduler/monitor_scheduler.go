package scheduler

import (
	"context"
	"sync"
	"time"

	"github.com/pika-monitor/pika/internal/service"
	"github.com/robfig/cron/v3"
	"go.uber.org/zap"
)

const monitorReconcileSpec = "@every 5m"

// MonitorScheduler 监控配置对账调度器。检测节奏由探针本地执行，服务端只负责
// 配置变更后的立即同步和周期性全量对账。
type MonitorScheduler struct {
	mu             sync.Mutex
	cron           *cron.Cron
	reconcileID    cron.EntryID
	monitorService *service.MonitorService
	logger         *zap.Logger
	ctx            context.Context
	cancel         context.CancelFunc
}

func NewMonitorScheduler(monitorService *service.MonitorService, logger *zap.Logger) *MonitorScheduler {
	return &MonitorScheduler{
		cron: cron.New(
			cron.WithSeconds(),
			cron.WithChain(
				cron.Recover(cron.DefaultLogger),
				cron.SkipIfStillRunning(cron.DefaultLogger),
			),
		),
		monitorService: monitorService,
		logger:         logger,
	}
}

func (s *MonitorScheduler) Start(ctx context.Context) {
	s.ctx, s.cancel = context.WithCancel(ctx)
	s.logger.Info("启动监控配置对账调度器")

	s.mu.Lock()
	if s.reconcileID == 0 {
		id, err := s.cron.AddFunc(monitorReconcileSpec, s.reconcile)
		if err != nil {
			s.logger.Error("添加监控对账任务失败", zap.Error(err))
		} else {
			s.reconcileID = id
		}
	}
	s.mu.Unlock()

	s.cron.Start()
	go s.reconcile()
}

func (s *MonitorScheduler) Stop() {
	if s.cancel != nil {
		s.cancel()
	}
	ctx := s.cron.Stop()
	<-ctx.Done()
	s.logger.Info("监控配置对账调度器已停止")
}

func (s *MonitorScheduler) reconcile() {
	if s.monitorService == nil {
		return
	}
	ctx := s.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	runCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := s.monitorService.ReconcileMonitorConfigs(runCtx); err != nil {
		s.logger.Error("对账下发监控配置失败", zap.Error(err))
	}
}

// AddTask 兼容旧接口。配置下发由 MonitorService 在 CRUD 路径完成。
func (s *MonitorScheduler) AddTask(monitorID string, interval int) error {
	_, _ = monitorID, interval
	return nil
}

func (s *MonitorScheduler) UpdateTask(monitorID string, interval int) error {
	_, _ = monitorID, interval
	return nil
}

func (s *MonitorScheduler) RemoveTask(monitorID string) {
	_ = monitorID
}

func (s *MonitorScheduler) GetTaskCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.reconcileID == 0 {
		return 0
	}
	return 1
}

func (s *MonitorScheduler) GetTaskStatus() map[string]interface{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	return map[string]interface{}{
		"totalTasks": s.cron.Entries(),
	}
}
