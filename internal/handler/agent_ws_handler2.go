package handler

import (
	"context"
	"net/http"
	"strings"

	ws "github.com/pika-monitor/pika/internal/websocket"
	"go.uber.org/zap"
)

// HandleWebSocket 处理WebSocket连接
func (h *AgentHandler) HandleWebSocket2(w http.ResponseWriter, r *http.Request) {
	h.handleWebSocket(w, r)
}

// HandleWebSocket 处理WebSocket连接
func (h *AgentHandler) handleWebSocket(w http.ResponseWriter, r *http.Request) error {
	conn, err := h.upgrader.Upgrade(w, r, nil)
	if err != nil {
		h.logger.Error("failed to upgrade websocket", zap.Error(err))
		return err
	}

	registerReq, err := h.readRegisterRequest(conn)
	if err != nil {
		conn.Close()
		return err
	}

	ip := strings.Split(conn.RemoteAddr().String(), `:`)[0]
	agent, err := h.agentService.RegisterAgent(context.Background(), ip, &registerReq.AgentInfo, registerReq.ApiKey)
	if err != nil {
		// 发送注册失败响应
		h.sendRegisterError(conn, err.Error())
		conn.Close()
		return err
	}

	// 先原子激活连接及其 BootID 会话，再发送注册响应。这样响应中的
	// AckSeq 与当前连接属于同一次会话切换，旧连接无法在其间污染位点。
	client := ws.NewClient(agent.ID, conn, h.wsManager)
	ackSeq := h.wsManager.Register(client, registerReq.BootID)
	if _, err := h.wsManager.DoIfCurrent(client, func() error {
		return h.agentService.UpdateAgentStatus(context.Background(), agent.ID, 1)
	}); err != nil {
		h.logger.Warn("failed to confirm agent online status", zap.String("agentID", agent.ID), zap.Error(err))
	}
	defer func() {
		h.wsManager.UnregisterThen(client, func() {
			h.markAgentOffline(client.ID)
		})
	}()

	// 发送注册成功响应：声明可靠投递支持，并回传该探针的累计确认
	// 位点，探针据此跳过已处理消息、只重放未确认部分
	if err := h.sendRegisterSuccess(conn, agent.ID, ackSeq); err != nil {
		h.logger.Error("failed to send register ack", zap.Error(err))
		conn.Close()
		return err
	}

	if agent.Enabled {
		// 下发防篡改配置
		if err := h.sendTamperConfig(conn, agent.ID); err != nil {
			h.logger.Error("failed to send tamper config", zap.Error(err))
			// 配置下发失败不中断连接，只记录日志
		}
		// 下发SSH登录监控配置
		if err := h.sendSSHLoginConfig(conn, agent.ID); err != nil {
			h.logger.Error("failed to send ssh login config", zap.Error(err))
			// 配置下发失败不中断连接，只记录日志
		}
		if err := h.sendPublicIPConfig(conn, agent.ID); err != nil {
			h.logger.Error("failed to send public ip config", zap.Error(err))
		}
		if err := h.sendMonitorConfig(conn, agent.ID); err != nil {
			h.logger.Error("failed to send monitor config", zap.Error(err))
		}
	}

	// 启动读写协程
	go client.WritePump()
	client.ReadPump(context.Background())
	return nil
}
