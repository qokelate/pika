package websocket

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"runtime/debug"

	"github.com/gorilla/websocket"
	"github.com/pika-monitor/pika/internal/protocol"
	"go.uber.org/zap"
)

const (
	serverPingInterval = 10 * time.Second
	serverPongWait     = 30 * time.Second
	serverWriteWait    = 10 * time.Second

	// 可靠事件必须保持会话内顺序；队列满表示服务端已无法及时消费，
	// 断开当前传输后由 agent 保留 outbox 并重连重放。
	reliableQueueSize = 512
	reliableQueueWait = 2 * time.Second

	// 普通遥测是 latest-wins，不应阻塞可靠事件。队列满时丢弃本批，
	// agent 下一轮会继续上报更新后的快照。
	bestEffortQueueSize = 128

	serverSendWait = 3 * time.Second

	messageRetryMin = 500 * time.Millisecond
	messageRetryMax = 10 * time.Second
)

// MessageHandler 处理一条 agent 上行消息。
type MessageHandler func(ctx context.Context, agentID string, messageType string, data json.RawMessage) error

// PongHandler 处理连接 pong。
type PongHandler func(client *Client)

// PermanentMessageError 表示重放也无法成功的坏消息，例如 JSON 结构错误。
// 可靠处理器会记录并确认这类消息，避免单条毒消息永久阻塞后续事件。
type PermanentMessageError struct {
	err error
}

func (e *PermanentMessageError) Error() string { return e.err.Error() }
func (e *PermanentMessageError) Unwrap() error { return e.err }

// Permanent 把不可重试的消息错误标记为永久错误。
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &PermanentMessageError{err: err}
}

func isPermanent(err error) bool {
	var target *PermanentMessageError
	return errors.As(err, &target)
}

type agentMessage struct {
	seq  uint64
	typ  string
	data json.RawMessage
}

// agentSession 是可靠投递状态的唯一所有者。它绑定 agentID + BootID，
// 与具体 WebSocket 连接解耦：同一进程重连复用 session，新进程注册时
// 原子替换 session，旧 worker 即使仍在返回路径上也无法提交 ACK。
type agentSession struct {
	agentID string
	bootID  string

	ctx    context.Context
	cancel context.CancelFunc

	reliableQueue   chan agentMessage
	bestEffortQueue chan agentMessage
	ackSeq          atomic.Uint64
}

func newAgentSession(agentID, bootID string) *agentSession {
	ctx, cancel := context.WithCancel(context.Background())
	return &agentSession{
		agentID:         agentID,
		bootID:          bootID,
		ctx:             ctx,
		cancel:          cancel,
		reliableQueue:   make(chan agentMessage, reliableQueueSize),
		bestEffortQueue: make(chan agentMessage, bestEffortQueueSize),
	}
}

// Client 只代表一次 WebSocket 传输，不拥有可靠会话状态。
type Client struct {
	ID      string
	conn    *websocket.Conn
	manager *Manager
	session *agentSession

	send chan []byte
	done chan struct{}

	closeOnce       sync.Once
	lastActive      atomic.Int64
	lastStatusWrite atomic.Int64
}

// NewClient 创建尚未注册到 Manager 的连接。
func NewClient(agentID string, conn *websocket.Conn, manager *Manager) *Client {
	c := &Client{
		ID:      agentID,
		conn:    conn,
		manager: manager,
		send:    make(chan []byte, 512),
		done:    make(chan struct{}),
	}
	c.touch()
	return c
}

func (c *Client) touch() {
	c.lastActive.Store(time.Now().UnixMilli())
}

func (c *Client) inactiveFor(now time.Time) time.Duration {
	last := c.lastActive.Load()
	if last == 0 {
		return 0
	}
	return now.Sub(time.UnixMilli(last))
}

func (c *Client) close() {
	c.closeOnce.Do(func() {
		close(c.done)
		if c.conn != nil {
			_ = c.conn.Close()
		}
	})
}

// Close 主动关闭本次传输；Handler 的退出路径负责同步注销 current client。
func (c *Client) Close() {
	c.close()
}

func (c *Client) sendWithTimeout(message []byte, timeout time.Duration) error {
	select {
	case <-c.done:
		return ErrClientNotFound
	default:
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-c.done:
		return ErrClientNotFound
	case c.send <- message:
		return nil
	case <-timer.C:
		return ErrSendTimeout
	}
}

func (c *Client) trySend(message []byte) bool {
	select {
	case <-c.done:
		return false
	default:
	}

	select {
	case <-c.done:
		return false
	case c.send <- message:
		return true
	default:
		return false
	}
}

// Manager 同步维护当前连接和每个 agent 的会话状态。
type Manager struct {
	mu       sync.RWMutex
	statusMu sync.Map // agentID → *sync.Mutex，串行化在线状态写库
	clients  map[string]*Client
	sessions map[string]*agentSession

	logger    *zap.Logger
	onMessage MessageHandler
	onPong    PongHandler
}

func NewManager(logger *zap.Logger) *Manager {
	return &Manager{
		clients:  make(map[string]*Client),
		sessions: make(map[string]*agentSession),
		logger:   logger,
	}
}

func (m *Manager) SetMessageHandler(handler MessageHandler) {
	m.onMessage = handler
}

func (m *Manager) SetPongHandler(handler PongHandler) {
	m.onPong = handler
}

// Run 只负责周期性连接保活检查和进程退出清理；注册/注销本身是同步的，
// 注册响应拿到的 ACK 与刚完成的会话切换因此属于同一个原子操作。
func (m *Manager) Run(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			m.shutdown()
			m.logger.Info("websocket manager stopped")
			return
		case <-ticker.C:
			m.checkInactiveClients()
		}
	}
}

// Register 原子激活连接并返回该 BootID 会话已确认的累计位点。
// 同 BootID 重连复用处理队列；BootID 变化创建全新 session 并取消旧 session。
func (m *Manager) Register(client *Client, bootID string) uint64 {
	var oldClient *Client
	var oldSession *agentSession
	var startSession *agentSession

	m.mu.Lock()
	oldClient = m.clients[client.ID]
	session := m.sessions[client.ID]
	if session == nil || (bootID != "" && session.bootID != bootID) {
		oldSession = session
		session = newAgentSession(client.ID, bootID)
		m.sessions[client.ID] = session
		startSession = session
	}
	client.session = session
	m.clients[client.ID] = client
	ackSeq := session.ackSeq.Load()
	clientCount := len(m.clients)
	m.mu.Unlock()

	// map/session 指针已切换后再关闭旧对象。旧 worker 的提交路径会先
	// 校验 session 指针，所以无法污染新会话。
	if oldSession != nil {
		oldSession.cancel()
	}
	if oldClient != nil && oldClient != client {
		oldClient.close()
	}
	if startSession != nil {
		go m.reliableProcessLoop(startSession)
		go m.bestEffortProcessLoop(startSession)
	}

	m.logger.Info("agent connected",
		zap.String("agentID", client.ID),
		zap.String("bootID", bootID),
		zap.Uint64("ackSeq", ackSeq),
		zap.Int("totalClients", clientCount))
	return ackSeq
}

func (m *Manager) lockAgentStatus(agentID string) func() {
	v, _ := m.statusMu.LoadOrStore(agentID, &sync.Mutex{})
	mu := v.(*sync.Mutex)
	mu.Lock()
	return mu.Unlock
}

func (m *Manager) unregisterLocked(client *Client) bool {
	m.mu.Lock()
	current := m.clients[client.ID] == client
	if current {
		delete(m.clients, client.ID)
	}
	clientCount := len(m.clients)
	m.mu.Unlock()

	client.close()
	if current {
		m.logger.Info("agent disconnected",
			zap.String("agentID", client.ID),
			zap.Int("totalClients", clientCount))
	}
	return current
}

// Unregister 只移除仍为 current 的连接，返回值用于决定是否标记离线。
func (m *Manager) Unregister(client *Client) bool {
	unlock := m.lockAgentStatus(client.ID)
	defer unlock()
	return m.unregisterLocked(client)
}

// UnregisterThen 在状态锁内完成注销和回调，避免离线写库与 pong/新连接在线写库交错。
func (m *Manager) UnregisterThen(client *Client, fn func()) bool {
	unlock := m.lockAgentStatus(client.ID)
	defer unlock()
	current := m.unregisterLocked(client)
	if current && fn != nil {
		fn()
	}
	return current
}

// ForgetAgent 删除已移除 agent 的连接与会话状态，终止常驻 worker。
func (m *Manager) ForgetAgent(agentID string) {
	unlock := m.lockAgentStatus(agentID)
	defer unlock()

	m.mu.Lock()
	client := m.clients[agentID]
	session := m.sessions[agentID]
	delete(m.clients, agentID)
	delete(m.sessions, agentID)
	m.mu.Unlock()

	if client != nil {
		client.close()
	}
	if session != nil {
		session.cancel()
	}
}

func (m *Manager) shutdown() {
	m.mu.Lock()
	clients := make([]*Client, 0, len(m.clients))
	for _, client := range m.clients {
		clients = append(clients, client)
	}
	sessions := make([]*agentSession, 0, len(m.sessions))
	for _, session := range m.sessions {
		sessions = append(sessions, session)
	}
	m.clients = make(map[string]*Client)
	m.sessions = make(map[string]*agentSession)
	m.mu.Unlock()

	for _, client := range clients {
		client.close()
	}
	for _, session := range sessions {
		session.cancel()
	}
}

func (m *Manager) isCurrentSession(session *agentSession) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.sessions[session.agentID] == session
}

func (m *Manager) currentClientForSession(session *agentSession) *Client {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.sessions[session.agentID] != session {
		return nil
	}
	client := m.clients[session.agentID]
	if client == nil || client.session != session {
		return nil
	}
	return client
}

func (m *Manager) checkInactiveClients() {
	now := time.Now()
	m.mu.RLock()
	inactive := make([]*Client, 0)
	for _, client := range m.clients {
		if client.inactiveFor(now) > 2*time.Minute {
			inactive = append(inactive, client)
		}
	}
	m.mu.RUnlock()

	for _, client := range inactive {
		m.mu.RLock()
		current := m.clients[client.ID] == client
		m.mu.RUnlock()
		if current {
			m.logger.Warn("agent inactive timeout, disconnecting", zap.String("agentID", client.ID))
			client.close()
		}
	}
}

func (m *Manager) SendToClient(agentID string, message []byte) error {
	m.mu.RLock()
	client := m.clients[agentID]
	m.mu.RUnlock()
	if client == nil {
		return ErrClientNotFound
	}
	return client.sendWithTimeout(message, serverSendWait)
}

func (m *Manager) TrySendToClient(agentID string, message []byte) error {
	m.mu.RLock()
	client := m.clients[agentID]
	m.mu.RUnlock()
	if client == nil {
		return ErrClientNotFound
	}
	if !client.trySend(message) {
		return ErrQueueFull
	}
	return nil
}

func (m *Manager) GetClient(agentID string) (*Client, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	client, ok := m.clients[agentID]
	return client, ok
}

// DoIfCurrent 在连接仍为 current 的前提下执行状态变更。
// 使用 per-agent 状态锁，避免持有连接表锁做 IO，同时保证 pong 写库
// 与 Unregister 离线写库互斥。
func (m *Manager) DoIfCurrent(client *Client, fn func() error) (bool, error) {
	unlock := m.lockAgentStatus(client.ID)
	defer unlock()
	m.mu.RLock()
	current := m.clients[client.ID] == client
	m.mu.RUnlock()
	if !current {
		return false, nil
	}
	return true, fn()
}

func (m *Manager) GetAllClients() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	ids := make([]string, 0, len(m.clients))
	for id := range m.clients {
		ids = append(ids, id)
	}
	return ids
}

func (m *Manager) ClientCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.clients)
}

func (m *Manager) AckSeq(agentID string) uint64 {
	m.mu.RLock()
	session := m.sessions[agentID]
	m.mu.RUnlock()
	if session == nil {
		return 0
	}
	return session.ackSeq.Load()
}

// enqueueBestEffort/ enqueueReliable 在持有 current-client 读锁时完成入队。
// Register 必须取得写锁，因此同 BootID 重连时，旧连接已经读出的消息必定
// 先入队，然后才会切换连接；切换完成后旧连接不能再把较大 seq 插到新连接
// 重放的较小 seq 前面，累计 ACK 顺序不会被跨连接竞态破坏。
func (m *Manager) enqueueBestEffort(client *Client, msg agentMessage) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.clients[client.ID] != client || m.sessions[client.ID] != client.session {
		return false
	}
	select {
	case client.session.bestEffortQueue <- msg:
		return true
	default:
		return false
	}
}

func (m *Manager) enqueueReliable(ctx context.Context, client *Client, msg agentMessage) error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.clients[client.ID] != client || m.sessions[client.ID] != client.session {
		return ErrClientNotFound
	}

	timer := time.NewTimer(reliableQueueWait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-client.done:
		return ErrClientNotFound
	case <-client.session.ctx.Done():
		return ErrClientNotFound
	case client.session.reliableQueue <- msg:
		return nil
	case <-timer.C:
		return ErrQueueFull
	}
}

// ReadPump 只解析并按可靠性分流。可靠队列提供反压；best-effort 遥测
// 队列满时直接丢弃，避免 VM 慢写阻塞安全事件。
func (c *Client) ReadPump(ctx context.Context) {
	defer c.close()
	if c.conn == nil {
		return
	}

	_ = c.conn.SetReadDeadline(time.Now().Add(serverPongWait))
	c.conn.SetPongHandler(func(string) error {
		if err := c.conn.SetReadDeadline(time.Now().Add(serverPongWait)); err != nil {
			return err
		}
		c.touch()
		if c.manager.onPong != nil {
			// 同步执行，确保连接退出后的离线写库一定发生在本次 pong
			// 状态刷新之后，避免遗留 goroutine 把已断开的连接重新标在线。
			c.manager.onPong(c)
		}
		return nil
	})

	for {
		_, raw, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				c.manager.logger.Error("websocket read error", zap.Error(err), zap.String("agentID", c.ID))
			}
			return
		}
		c.touch()

		var msg protocol.InputMessage
		if err := json.Unmarshal(raw, &msg); err != nil {
			c.manager.logger.Error("failed to parse message", zap.Error(err), zap.String("agentID", c.ID))
			continue
		}

		item := agentMessage{seq: msg.Seq, typ: string(msg.Type), data: msg.Data}
		if msg.Seq == 0 {
			if !c.manager.enqueueBestEffort(c, item) {
				c.manager.logger.Warn("best-effort agent queue full, dropping telemetry",
					zap.String("agentID", c.ID), zap.String("type", string(msg.Type)))
			}
			continue
		}

		if err := c.manager.enqueueReliable(ctx, c, item); err != nil {
			c.manager.logger.Warn("reliable agent queue stalled, disconnecting for replay",
				zap.String("agentID", c.ID), zap.Uint64("seq", msg.Seq), zap.Error(err))
			return
		}
	}
}

func (m *Manager) bestEffortProcessLoop(session *agentSession) {
	for {
		select {
		case <-session.ctx.Done():
			return
		case msg := <-session.bestEffortQueue:
			if session.ctx.Err() != nil || !m.isCurrentSession(session) {
				return
			}
			m.handleBestEffort(session, msg)
		}
	}
}

func (m *Manager) handleBestEffort(session *agentSession, msg agentMessage) {
	defer func() {
		if r := recover(); r != nil {
			m.logger.Error("best-effort handler panic",
				zap.Any("panic", r),
				zap.ByteString("stack", debug.Stack()),
				zap.String("agentID", session.agentID),
				zap.String("type", msg.typ))
		}
	}()
	if m.onMessage == nil {
		return
	}
	ctx, cancel := context.WithTimeout(session.ctx, 10*time.Second)
	defer cancel()
	if err := m.onMessage(ctx, session.agentID, msg.typ, msg.data); err != nil {
		m.logger.Error("failed to handle best-effort message, dropped",
			zap.Error(err), zap.String("agentID", session.agentID), zap.String("type", msg.typ))
	}
}

func (m *Manager) reliableProcessLoop(session *agentSession) {
	for {
		select {
		case <-session.ctx.Done():
			return
		case msg := <-session.reliableQueue:
			if session.ctx.Err() != nil || !m.isCurrentSession(session) {
				return
			}
			if msg.seq <= session.ackSeq.Load() {
				m.sendAck(session, session.ackSeq.Load())
				continue
			}

			if !m.processReliableMessage(session, msg) {
				return
			}
			if !m.isCurrentSession(session) {
				return
			}
			session.ackSeq.Store(msg.seq)
			m.sendAck(session, msg.seq)
		}
	}
}

// processReliableMessage 对瞬时错误原位退避重试，保证后续序号不能越过
// 失败消息；永久格式错误明确丢弃并允许推进位点。
func (m *Manager) processReliableMessage(session *agentSession, msg agentMessage) (ok bool) {
	defer func() {
		if r := recover(); r != nil {
			m.logger.Error("reliable handler panic, dropped",
				zap.Any("panic", r),
				zap.ByteString("stack", debug.Stack()),
				zap.String("agentID", session.agentID),
				zap.String("type", msg.typ),
				zap.Uint64("seq", msg.seq))
			ok = true
		}
	}()

	retryDelay := messageRetryMin
	for {
		if session.ctx.Err() != nil || !m.isCurrentSession(session) {
			return false
		}
		if m.onMessage == nil {
			return true
		}

		ctx, cancel := context.WithTimeout(session.ctx, 15*time.Second)
		err := m.onMessage(ctx, session.agentID, msg.typ, msg.data)
		cancel()
		if err == nil {
			return true
		}
		if isPermanent(err) {
			m.logger.Error("permanent reliable message error, dropped",
				zap.Error(err), zap.String("agentID", session.agentID),
				zap.String("type", msg.typ), zap.Uint64("seq", msg.seq))
			return true
		}

		m.logger.Warn("reliable message handling failed, retrying",
			zap.Error(err), zap.String("agentID", session.agentID),
			zap.String("type", msg.typ), zap.Uint64("seq", msg.seq),
			zap.Duration("retryAfter", retryDelay))

		timer := time.NewTimer(retryDelay)
		select {
		case <-session.ctx.Done():
			timer.Stop()
			return false
		case <-timer.C:
		}
		if retryDelay < messageRetryMax {
			retryDelay *= 2
			if retryDelay > messageRetryMax {
				retryDelay = messageRetryMax
			}
		}
	}
}

func (m *Manager) sendAck(session *agentSession, seq uint64) {
	payload, err := json.Marshal(protocol.OutboundMessage{
		Type: protocol.MessageTypeAck,
		Data: protocol.AckData{Seq: seq},
	})
	if err != nil {
		return
	}
	if client := m.currentClientForSession(session); client != nil {
		client.trySend(payload)
	}
}

func (c *Client) WritePump() {
	if c.conn == nil {
		return
	}
	ticker := time.NewTicker(serverPingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.done:
			return
		case message := <-c.send:
			_ = c.conn.SetWriteDeadline(time.Now().Add(serverWriteWait))
			if err := c.conn.WriteMessage(websocket.TextMessage, message); err != nil {
				c.manager.logger.Error("failed to write message", zap.Error(err), zap.String("agentID", c.ID))
				c.close()
				return
			}
		case <-ticker.C:
			_ = c.conn.SetWriteDeadline(time.Now().Add(serverWriteWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				c.close()
				return
			}
		}
	}
}

func (c *Client) ShouldWriteStatus(refreshInterval time.Duration) bool {
	now := time.Now().UnixMilli()
	last := c.lastStatusWrite.Load()
	return last == 0 || now-last >= refreshInterval.Milliseconds()
}

func (c *Client) MarkStatusWritten() {
	c.lastStatusWrite.Store(time.Now().UnixMilli())
}

var (
	ErrClientNotFound = &websocket.CloseError{Code: 1000, Text: "client not found"}
	ErrSendTimeout    = &websocket.CloseError{Code: 1001, Text: "send timeout"}
	ErrQueueFull      = &websocket.CloseError{Code: 1013, Text: "reliable queue full"}
)
