package service

import (
	"context"
	"encoding/json"
	"errors"
	"sync"

	"github.com/pika-monitor/pika/internal/models"
	"github.com/pika-monitor/pika/internal/protocol"
	ws "github.com/pika-monitor/pika/internal/websocket"
	"go.uber.org/zap"
)

const monitorConfigSendConcurrency = 32

func buildMonitorItem(monitor models.MonitorTask) protocol.MonitorItem {
	item := protocol.MonitorItem{
		ID:       monitor.ID,
		Type:     monitor.Type,
		Target:   monitor.Target,
		Interval: monitor.Interval,
	}
	switch monitor.Type {
	case "http", "https":
		cfg := monitor.HTTPConfig.Data()
		item.HTTPConfig = &cfg
	case "tcp":
		cfg := monitor.TCPConfig.Data()
		item.TCPConfig = &cfg
	case "icmp", "ping":
		cfg := monitor.ICMPConfig.Data()
		item.ICMPConfig = &cfg
	}
	return item
}

func cloneMonitorTask(task models.MonitorTask) models.MonitorTask {
	task.AgentIds = append([]string(nil), task.AgentIds...)
	task.Tags = append([]string(nil), task.Tags...)
	return task
}

func (s *MonitorService) onlineEnabledAgentSet(ctx context.Context) map[string]struct{} {
	connected := map[string]struct{}{}
	if s.wsManager != nil {
		for _, id := range s.wsManager.GetAllClients() {
			connected[id] = struct{}{}
		}
	}
	if len(connected) == 0 {
		return connected
	}

	enabledAgents, err := s.agentRepo.FindEnabledAgents(ctx)
	if err != nil {
		s.logger.Warn("查询启用探针失败，回退下发到全部在线连接", zap.Error(err))
		return connected
	}
	enabled := make(map[string]struct{}, len(enabledAgents))
	for _, agent := range enabledAgents {
		if _, ok := connected[agent.ID]; ok {
			enabled[agent.ID] = struct{}{}
		}
	}
	return enabled
}

func (s *MonitorService) affectedAgentIDs(ctx context.Context, monitors ...models.MonitorTask) []string {
	online := s.onlineEnabledAgentSet(ctx)
	if len(online) == 0 {
		return nil
	}

	seen := make(map[string]struct{})
	var ids []string
	add := func(id string) {
		if _, ok := online[id]; !ok {
			return
		}
		if _, ok := seen[id]; ok {
			return
		}
		seen[id] = struct{}{}
		ids = append(ids, id)
	}

	for i := range monitors {
		set, err := resolveMonitorTargetSet(ctx, s.agentRepo, &monitors[i])
		if err != nil {
			s.logger.Warn("解析监控目标探针失败", zap.String("monitorID", monitors[i].ID), zap.Error(err))
			continue
		}
		if set.all {
			for id := range online {
				add(id)
			}
			continue
		}
		for id := range set.ids {
			add(id)
		}
	}
	return ids
}

func (s *MonitorService) syncAffectedAgents(ctx context.Context, monitors ...models.MonitorTask) {
	s.SyncMonitorConfigToAgents(ctx, s.affectedAgentIDs(ctx, monitors...))
}

// ReconcileMonitorConfigs 向所有在线启用探针下发其完整监控任务表。
func (s *MonitorService) ReconcileMonitorConfigs(ctx context.Context) error {
	online := s.onlineEnabledAgentSet(ctx)
	ids := make([]string, 0, len(online))
	for id := range online {
		ids = append(ids, id)
	}
	s.SyncMonitorConfigToAgents(ctx, ids)
	return nil
}

// SyncMonitorConfigToAgent 向单个探针下发完整监控配置。
func (s *MonitorService) SyncMonitorConfigToAgent(ctx context.Context, agentID string) {
	s.SyncMonitorConfigToAgents(ctx, []string{agentID})
}

// MonitorConfigForAgent 构造指定探针当前应执行的完整监控配置。
func (s *MonitorService) MonitorConfigForAgent(ctx context.Context, agentID string) (protocol.MonitorConfigPayload, error) {
	payloads, err := s.buildMonitorConfigs(ctx, []string{agentID})
	if err != nil {
		return protocol.MonitorConfigPayload{Replace: true}, err
	}
	payload, ok := payloads[agentID]
	if !ok {
		return protocol.MonitorConfigPayload{Replace: true}, nil
	}
	return payload, nil
}

func (s *MonitorService) buildMonitorConfigs(ctx context.Context, agentIDs []string) (map[string]protocol.MonitorConfigPayload, error) {
	result := make(map[string]protocol.MonitorConfigPayload, len(agentIDs))
	for _, id := range agentIDs {
		result[id] = protocol.MonitorConfigPayload{Replace: true}
	}
	if len(agentIDs) == 0 {
		return result, nil
	}

	monitors, err := s.FindByEnabled(ctx, true)
	if err != nil {
		return result, err
	}

	agentSet := make(map[string]struct{}, len(agentIDs))
	for _, id := range agentIDs {
		agentSet[id] = struct{}{}
	}

	itemsByAgent := make(map[string][]protocol.MonitorItem, len(agentIDs))
	for i := range monitors {
		item := buildMonitorItem(monitors[i])
		set, err := resolveMonitorTargetSet(ctx, s.agentRepo, &monitors[i])
		if err != nil {
			s.logger.Warn("解析监控目标探针失败", zap.String("monitorID", monitors[i].ID), zap.Error(err))
			continue
		}
		if set.all {
			for _, id := range agentIDs {
				itemsByAgent[id] = append(itemsByAgent[id], item)
			}
			continue
		}
		for id := range set.ids {
			if _, ok := agentSet[id]; ok {
				itemsByAgent[id] = append(itemsByAgent[id], item)
			}
		}
	}

	for id, items := range itemsByAgent {
		result[id] = protocol.MonitorConfigPayload{Replace: true, Items: items}
	}
	return result, nil
}

// SyncMonitorConfigToAgents 向指定探针下发完整监控配置（Replace=true）。
func (s *MonitorService) SyncMonitorConfigToAgents(ctx context.Context, agentIDs []string) {
	if s.wsManager == nil || len(agentIDs) == 0 {
		return
	}

	payloads, err := s.buildMonitorConfigs(ctx, agentIDs)
	if err != nil {
		s.logger.Error("构建监控配置失败", zap.Error(err))
		return
	}

	sem := make(chan struct{}, monitorConfigSendConcurrency)
	var wg sync.WaitGroup
	for _, agentID := range agentIDs {
		agentID := agentID
		payload := payloads[agentID]
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			if err := s.sendMonitorConfigToAgent(agentID, payload); err != nil {
				if errors.Is(err, ws.ErrClientNotFound) || errors.Is(err, ws.ErrQueueFull) {
					return
				}
				s.logger.Warn("下发监控配置失败",
					zap.String("agentID", agentID),
					zap.Error(err))
			}
		}()
	}
	wg.Wait()
}

func (s *MonitorService) sendMonitorConfigToAgent(agentID string, payload protocol.MonitorConfigPayload) error {
	msgData, err := json.Marshal(protocol.OutboundMessage{
		Type: protocol.MessageTypeMonitorConfig,
		Data: payload,
	})
	if err != nil {
		return err
	}
	return s.wsManager.TrySendToClient(agentID, msgData)
}

func (s *MonitorService) ClearMonitorConfig(agentID string) {
	if s.wsManager == nil || agentID == "" {
		return
	}
	if err := s.sendMonitorConfigToAgent(agentID, protocol.MonitorConfigPayload{Replace: true}); err != nil {
		if errors.Is(err, ws.ErrClientNotFound) || errors.Is(err, ws.ErrQueueFull) {
			return
		}
		s.logger.Warn("清空探针监控配置失败", zap.String("agentID", agentID), zap.Error(err))
	}
}
