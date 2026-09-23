package service

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/go-orz/orz"
	"github.com/google/uuid"
	"github.com/pika-monitor/pika/internal/metric"
	"github.com/pika-monitor/pika/internal/models"
	"github.com/pika-monitor/pika/internal/protocol"
	"github.com/pika-monitor/pika/internal/repo"
	ws "github.com/pika-monitor/pika/internal/websocket"
	"go.uber.org/zap"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

type MonitorService struct {
	logger *zap.Logger
	*repo.MonitorRepo
	*orz.Service
	agentRepo     *repo.AgentRepo
	metricService *MetricService
	wsManager     *ws.Manager

	// 调度器引用（用于动态管理任务）
	scheduler MonitorScheduler
}

// MonitorScheduler 调度器接口（避免循环依赖）
type MonitorScheduler interface {
	AddTask(monitorID string, interval int) error
	UpdateTask(monitorID string, interval int) error
	RemoveTask(monitorID string)
}

func NewMonitorService(logger *zap.Logger, db *gorm.DB, metricService *MetricService, wsManager *ws.Manager) *MonitorService {
	return &MonitorService{
		logger:        logger,
		Service:       orz.NewService(db),
		MonitorRepo:   repo.NewMonitorRepo(db),
		agentRepo:     repo.NewAgentRepo(db),
		metricService: metricService,
		wsManager:     wsManager,
	}
}

// SetScheduler 设置调度器（由外部注入，避免循环依赖）
func (s *MonitorService) SetScheduler(scheduler MonitorScheduler) {
	s.scheduler = scheduler
}

type MonitorTaskRequest struct {
	Name             string                     `json:"name"`
	Type             string                     `json:"type"`
	Target           string                     `json:"target"`
	Description      string                     `json:"description"`
	Enabled          bool                       `json:"enabled,omitempty"`
	ShowTargetPublic bool                       `json:"showTargetPublic,omitempty"` // 在公开页面是否显示目标地址
	Visibility       string                     `json:"visibility,omitempty"`       // 可见性: public-匿名可见, private-登录可见
	Interval         int                        `json:"interval"`                   // 检测频率（秒）
	HTTPConfig       protocol.HTTPMonitorConfig `json:"httpConfig,omitempty"`
	TCPConfig        protocol.TCPMonitorConfig  `json:"tcpConfig,omitempty"`
	ICMPConfig       protocol.ICMPMonitorConfig `json:"icmpConfig,omitempty"`
	AgentIds         []string                   `json:"agentIds,omitempty"`
	Tags             []string                   `json:"tags,omitempty"` // 标签列表，拥有这些标签的探针都会执行此监控
}

func (s *MonitorService) CreateMonitor(ctx context.Context, req *MonitorTaskRequest) (*models.MonitorTask, error) {
	// 设置默认检测频率
	interval := req.Interval
	if interval <= 0 {
		interval = 60 // 默认 60 秒
	}

	// 设置默认可见性
	visibility := req.Visibility
	if visibility == "" {
		visibility = "public" // 默认公开可见
	}

	task := &models.MonitorTask{
		ID:               uuid.NewString(),
		Name:             strings.TrimSpace(req.Name),
		Type:             req.Type,
		Target:           strings.TrimSpace(req.Target),
		Description:      req.Description,
		Enabled:          req.Enabled,
		ShowTargetPublic: req.ShowTargetPublic,
		Visibility:       visibility,
		Interval:         interval,
		AgentIds:         datatypes.JSONSlice[string](req.AgentIds),
		Tags:             datatypes.JSONSlice[string](req.Tags),
		HTTPConfig:       datatypes.NewJSONType(req.HTTPConfig),
		TCPConfig:        datatypes.NewJSONType(req.TCPConfig),
		ICMPConfig:       datatypes.NewJSONType(req.ICMPConfig),
		CreatedAt:        0,
		UpdatedAt:        0,
	}

	if err := s.MonitorRepo.Create(ctx, task); err != nil {
		return nil, err
	}

	if task.Enabled {
		s.syncAffectedAgents(ctx, *task)
		if s.scheduler != nil {
			if err := s.scheduler.AddTask(task.ID, task.Interval); err != nil {
				s.logger.Error("添加监控任务到调度器失败",
					zap.String("taskID", task.ID),
					zap.Error(err))
			}
		}
	}

	return task, nil
}

func (s *MonitorService) UpdateMonitor(ctx context.Context, id string, req *MonitorTaskRequest) (*models.MonitorTask, error) {
	task, err := s.MonitorRepo.FindById(ctx, id)
	if err != nil {
		return nil, err
	}

	oldTask := cloneMonitorTask(task)
	oldEnabled := task.Enabled
	oldInterval := task.Interval

	task.Enabled = req.Enabled
	task.Name = strings.TrimSpace(req.Name)
	task.Type = req.Type
	task.Target = strings.TrimSpace(req.Target)
	task.Description = req.Description
	task.ShowTargetPublic = req.ShowTargetPublic
	task.Visibility = req.Visibility

	// 更新检测频率
	interval := req.Interval
	if interval <= 0 {
		interval = 60 // 默认 60 秒
	}
	task.Interval = interval

	task.AgentIds = req.AgentIds
	task.Tags = req.Tags
	task.HTTPConfig = datatypes.NewJSONType(req.HTTPConfig)
	task.TCPConfig = datatypes.NewJSONType(req.TCPConfig)
	task.ICMPConfig = datatypes.NewJSONType(req.ICMPConfig)

	if err := s.MonitorRepo.Save(ctx, &task); err != nil {
		return nil, err
	}

	// 清理监控缓存中不再关联的探针数据
	if s.metricService != nil {
		if err := s.metricService.CleanMonitorCache(ctx, id); err != nil {
			s.logger.Warn("清理监控缓存失败",
				zap.String("monitorID", id),
				zap.Error(err))
			// 不返回错误，继续执行
		}
	}

	// 更新调度器
	if s.scheduler != nil {
		// 如果从禁用变为启用，或者间隔时间改变
		if !oldEnabled && task.Enabled {
			// 添加任务到调度器
			if err := s.scheduler.AddTask(task.ID, task.Interval); err != nil {
				s.logger.Error("添加监控任务到调度器失败",
					zap.String("taskID", task.ID),
					zap.Error(err))
			}
		} else if oldEnabled && !task.Enabled {
			// 从调度器中移除任务
			s.scheduler.RemoveTask(task.ID)
		} else if task.Enabled && oldInterval != task.Interval {
			if err := s.scheduler.UpdateTask(task.ID, task.Interval); err != nil {
				s.logger.Error("更新监控任务调度器失败",
					zap.String("taskID", task.ID),
					zap.Error(err))
			}
		}
	}

	s.syncAffectedAgents(ctx, oldTask, task)
	return &task, nil
}

func (s *MonitorService) DeleteMonitor(ctx context.Context, id string) error {
	task, findErr := s.MonitorRepo.FindById(ctx, id)
	err := s.Transaction(ctx, func(ctx context.Context) error {
		return s.MonitorRepo.DeleteById(ctx, id)
	})
	if err != nil {
		return err
	}

	if s.scheduler != nil {
		s.scheduler.RemoveTask(id)
	}
	if findErr == nil {
		s.syncAffectedAgents(ctx, task)
	} else {
		_ = s.ReconcileMonitorConfigs(ctx)
	}
	return nil
}

// ListByAuth 返回公开展示所需的监控配置和汇总统计
func (s *MonitorService) ListByAuth(ctx context.Context, isAuthenticated bool) ([]metric.PublicMonitorOverview, error) {
	// 获取符合权限的监控任务列表
	monitors, err := s.FindByAuth(ctx, isAuthenticated)
	if err != nil {
		return nil, err
	}

	// 构建监控概览列表
	items := make([]metric.PublicMonitorOverview, 0, len(monitors))
	for _, monitor := range monitors {
		// 查询统计数据
		stats := s.metricService.GetMonitorStatsForTask(ctx, &monitor)
		item := s.buildMonitorOverview(monitor, stats)
		items = append(items, item)
	}

	return items, nil
}

// GetMonitorSparklinesByAuth 返回当前访问者可见监控项的批量走势图。
func (s *MonitorService) GetMonitorSparklinesByAuth(ctx context.Context, isAuthenticated bool) (*metric.PublicMonitorSparklinesResponse, error) {
	monitors, err := s.FindByAuth(ctx, isAuthenticated)
	if err != nil {
		return nil, err
	}

	monitorIDs := make([]string, 0, len(monitors))
	for _, monitor := range monitors {
		monitorIDs = append(monitorIDs, monitor.ID)
	}

	queryCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	generatedAt, items, err := s.metricService.GetCachedMonitorSparklines(queryCtx, monitorIDs, time.Hour, 20*time.Second)
	if err != nil {
		return nil, err
	}

	return &metric.PublicMonitorSparklinesResponse{GeneratedAt: generatedAt, Items: items}, nil
}

// buildMonitorOverview 构建监控概览对象
func (s *MonitorService) buildMonitorOverview(monitor models.MonitorTask, stats *metric.MonitorStatsResult) metric.PublicMonitorOverview {
	// 根据 ShowTargetPublic 字段决定是否返回真实的 Target
	target := monitor.Target
	if !monitor.ShowTargetPublic {
		target = "******"
	}

	overview := metric.PublicMonitorOverview{
		ID:               monitor.ID,
		Name:             monitor.Name,
		Type:             monitor.Type,
		Target:           target,
		ShowTargetPublic: monitor.ShowTargetPublic,
		Description:      monitor.Description,
		Enabled:          monitor.Enabled,
		Interval:         monitor.Interval,
		AgentCount:       stats.AgentCount,
		Status:           stats.Status,
		ResponseTime:     stats.ResponseTime,
		ResponseTimeMin:  stats.ResponseTimeMin,
		ResponseTimeMax:  stats.ResponseTimeMax,
		CertExpiryTime:   stats.CertExpiryTime,
		CertDaysLeft:     stats.CertDaysLeft,
		LastCheckTime:    stats.LastCheckTime,
	}

	// 复制探针状态分布
	overview.AgentStats = stats.AgentStats

	return overview
}

// GetMonitorStatsByID 获取监控任务的统计数据（聚合后的单个监控详情）
func (s *MonitorService) GetMonitorStatsByID(ctx context.Context, monitorID string) (*metric.PublicMonitorOverview, error) {
	// 查询监控任务
	monitor, err := s.MonitorRepo.FindById(ctx, monitorID)
	if err != nil {
		return nil, err
	}

	// 查询统计数据
	stats := s.metricService.GetMonitorStats(ctx, monitorID)
	// 构建监控概览对象
	overview := s.buildMonitorOverview(monitor, stats)

	return &overview, nil
}

// GetMonitorByAuth 根据认证状态获取监控任务（已登录返回全部，未登录返回公开可见）
func (s *MonitorService) GetMonitorByAuth(ctx context.Context, id string, isAuthenticated bool) (*models.MonitorTask, error) {
	if isAuthenticated {
		monitor, err := s.MonitorRepo.FindById(ctx, id)
		if err != nil {
			return nil, err
		}
		if !monitor.Enabled {
			return nil, fmt.Errorf("monitor is disabled")
		}
		return &monitor, nil
	}
	monitor, err := s.MonitorRepo.FindPublicMonitorByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if !monitor.Enabled {
		return nil, fmt.Errorf("monitor is disabled")
	}
	return monitor, nil
}

// GetLatestMonitorMetricsByType 获取指定类型的最新监控指标（用于告警检查）
func (s *MonitorService) GetLatestMonitorMetricsByType(ctx context.Context, monitorType string) ([]protocol.MonitorData, error) {
	// 查询数据库
	monitorTasks, err := s.FindByEnabledAndType(ctx, true, monitorType)
	if err != nil {
		return nil, err
	}

	// 在缓存中查询最新的监控数据
	var result []protocol.MonitorData
	for _, task := range monitorTasks {
		monitorData := s.metricService.GetMonitorAgentStatsForTask(ctx, &task)
		for i := range monitorData {
			monitorData[i].MonitorName = task.Name
		}
		result = append(result, monitorData...)
	}

	return result, nil
}

func (s *MonitorService) GetAllLatestMonitorMetrics(ctx context.Context) ([]protocol.MonitorData, error) {
	monitorTasks, err := s.FindByEnabled(ctx, true)
	if err != nil {
		return nil, err
	}

	var result []protocol.MonitorData
	for _, task := range monitorTasks {
		monitorData := s.metricService.GetMonitorAgentStatsForTask(ctx, &task)
		for i := range monitorData {
			monitorData[i].MonitorName = task.Name
		}
		result = append(result, monitorData...)
	}
	return result, nil
}
