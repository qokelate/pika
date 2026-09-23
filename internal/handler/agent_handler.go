package handler

import (
	"sync"

	"github.com/go-orz/orz"
	"github.com/gorilla/websocket"
	"github.com/labstack/echo/v5"
	"github.com/pika-monitor/pika/internal/service"
	ws "github.com/pika-monitor/pika/internal/websocket"
	"go.uber.org/zap"
)

// Enable 启用探针数据处理。
func (h *AgentHandler) Enable(c *echo.Context) error {
	return h.updateEnabled(c, true)
}

// Disable 禁用探针数据处理和告警。
func (h *AgentHandler) Disable(c *echo.Context) error {
	return h.updateEnabled(c, false)
}

func (h *AgentHandler) updateEnabled(c *echo.Context, enabled bool) error {
	h.enabledMu.Lock()
	defer h.enabledMu.Unlock()

	agentID := c.Param("id")
	if err := h.agentService.UpdateAgentEnabled(c.Request().Context(), agentID, enabled); err != nil {
		return err
	}
	if enabled {
		h.monitorSvc.SyncMonitorConfigToAgent(c.Request().Context(), agentID)
	} else {
		h.monitorSvc.ClearMonitorConfig(agentID)
	}
	return orz.Ok(c, orz.Map{})
}

type AgentHandler struct {
	enabledMu       sync.RWMutex
	logger          *zap.Logger
	agentService    *service.AgentService
	trafficService  *service.TrafficService
	metricService   *service.MetricService
	monitorSvc      *service.MonitorService
	tamperService   *service.TamperService
	ddnsService     *service.DDNSService
	sshLoginService *service.SSHLoginService
	apiKeyService   *service.ApiKeyService
	propertyService *service.PropertyService
	wsManager       *ws.Manager
	upgrader        websocket.Upgrader
}

func NewAgentHandler(logger *zap.Logger, agentService *service.AgentService, trafficService *service.TrafficService,
	metricService *service.MetricService, monitorService *service.MonitorService, tamperService *service.TamperService,
	ddnsService *service.DDNSService, sshLoginService *service.SSHLoginService, apiKeyService *service.ApiKeyService,
	propertyService *service.PropertyService, wsManager *ws.Manager) *AgentHandler {

	h := &AgentHandler{
		logger:          logger,
		agentService:    agentService,
		trafficService:  trafficService,
		metricService:   metricService,
		monitorSvc:      monitorService,
		tamperService:   tamperService,
		ddnsService:     ddnsService,
		sshLoginService: sshLoginService,
		apiKeyService:   apiKeyService,
		propertyService: propertyService,
		wsManager:       wsManager,
	}

	// 初始化upgrader，需要在创建handler之后因为需要引用h.checkOrigin
	h.upgrader = websocket.Upgrader{
		ReadBufferSize:    1024 * 8,
		WriteBufferSize:   1024 * 8,
		EnableCompression: false,
	}

	// 设置WebSocket消息处理器
	wsManager.SetMessageHandler(h.handleWebSocketMessage)
	wsManager.SetPongHandler(h.handleWebSocketPong)

	return h
}
