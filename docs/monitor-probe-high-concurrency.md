# 服务监控（探针检测）高并发优化建议

本文针对当前「服务监控 / 探针检测」链路：服务端按间隔下发检测任务，探针执行 HTTP/TCP/ICMP 检查并回传结果。线上典型问题是 **进程容易直接崩溃（fatal / panic）**，以及 **探针数量上来后吞吐、延迟、连接稳定性急剧下降**。

相关代码主要在：

- 服务端调度：`internal/scheduler/monitor_scheduler.go`
- 任务下发：`internal/service/monitor_service.go`
- 结果缓存 / 写入：`internal/service/metric_service.go`
- 连接与消息泵：`internal/websocket/manager.go`、`internal/handler/agent_ws_handler.go`
- 探针执行：`pkg/agent/service/agent.go`、`pkg/agent/collector/monitor.go`

---

## 1. 现状流量模型

当前实现是 **服务端远程触发，而不是探针本地调度**：

1. `MonitorScheduler` 为每个启用的监控任务注册 `@every Ns` cron。
2. 到期后从数据库读取任务，解析目标探针（指定 ID ∪ 标签；都未指定则发给所有在线探针）。
3. 对每个目标探针单独发送一条 `monitor_config`，payload 里通常只有 **1 个监控项**，且 `Interval` 固定为 `0`。
4. 探针 `readLoop` 对每条配置 `go handleMonitorConfig(...)`，立刻执行检测。
5. 检测结果走 **可靠 outbox**（带 seq）作为 `metrics` 上报。
6. 服务端按探针串行处理消息，写入内存缓存和 VictoriaMetrics。

因此单轮检测的消息量近似：

```text
下发次数 ≈ 监控任务数 × 目标探针数
回传次数 ≈ 下发次数
```

例如 200 个探针、50 个监控项、间隔 30 秒，且任务未指定探针/标签（对所有探针生效）：

| 方向 | 估算 |
| --- | --- |
| 服务端每分钟下发 | `50 × 200 × 2 = 20,000` 条 WebSocket 消息 |
| 探针每分钟回传 | 约 20,000 条带 seq 的可靠消息 |
| 同时在途检测 | 峰值可到数千个 goroutine / 连接 / ICMP 进程 |

这套模型在探针少、任务少时可用；探针一多，调度、执行、上报、写库会同时被放大。

---

## 2. 问题全景

按影响排序：

| 优先级 | 问题 | 后果 |
| --- | --- | --- |
| P0 | `SafeMap.Keys()/Values()` 无快照，并发迭代 + 写入 | 服务端 **fatal**，整个进程退出 |
| P0 | 探针监控 goroutine 无 `recover`，ICMP 复用同一 `Pinger` 重试 | 探针进程崩溃 |
| P0 | HTTP 响应体无上限，超时/次数无上限 | OOM / fd 耗尽 |
| P1 | 每次检测都由服务端触发，而不是配置下发后本地调度 | 消息量随「任务 × 探针」平方级增长 |
| P1 | 监控结果走可靠 outbox，和安全事件抢同一队列 | 可靠队列堵死、断线重放、安全事件延迟 |
| P1 | cron 任务可重叠，且对探针串行 `SendToClient`（最多等 3s） | 调度堆积，下发延迟数十秒到数分钟 |
| P2 | 全局锁持有期间做 DB / VM IO | 注册、消息入队、pong 互相卡住 |
| P2 | 每条指标单独写 VictoriaMetrics | 写入放大，best-effort 队列丢数 |
| P2 | 告警 / 列表接口对每个监控任务打库 | 页面和告警周期被 DB 拖住 |

---

## 3. P0：先消灭会把进程打死的问题

Go 里 **concurrent map iteration and map write 是 fatal**，不能被 `recover()` 接住。HTTP 的 `middleware.Recover()` 也覆盖不到 WebSocket worker、cron、告警 ticker。

### 3.1 `SafeMap` 迭代器在解锁后遍历活 map

`github.com/go-orz/toolkit/syncx.SafeMap` 当前实现：

```go
func (m *SafeMap[K, V]) Keys() iter.Seq[K] {
    m.mu.RLock()
    defer m.mu.RUnlock()
    return maps.Keys(m.m) // 返回的是惰性迭代器
}
```

`Keys()` / `Values()` 返回时锁已经释放，调用方 `for range` 时仍在遍历底层 map。与此同时探针上报会 `Agents.Set()`。

触发路径非常容易碰上：

- 写：`MetricService.updateMonitorCache()` 被大量探针并发调用
- 读：公开服务列表、监控详情、告警 `CheckMonitorAlerts`（每 30s）调用 `GetMonitorStats` / `GetMonitorAgentStats`

这是「探针一多就容易崩」的第一嫌疑。

**建议：**

- 不要返回活 map 的 `iter.Seq`。改为拷贝快照：

```go
func (m *SafeMap[K, V]) Snapshot() map[K]V {
    m.mu.RLock()
    defer m.mu.RUnlock()
    out := make(map[K]V, len(m.m))
    for k, v := range m.m {
        out[k] = v
    }
    return out
}
```

- 监控缓存读路径全部改为 snapshot，禁止边迭代边 `Delete`。
- `GetMonitorAgentStats` 不要原地改缓存对象的 `AgentName`，先拷贝再填名称，避免和上报写并发。
- 对 `stat == nil` 做防护，避免可恢复 panic。
- 如果暂时不改 toolkit，至少在 `metric_service` 内包一层 snapshot helper，停止直接 `range xxx.Keys()`。

### 3.2 监控缓存 Get-or-Create 丢失更新

```go
latestMetrics, ok := s.monitorLatestCache.Get(monitorID)
if !ok {
    latestMetrics = &metric.LatestMonitorMetrics{Agents: syncx.NewSafeMap[...]}
}
latestMetrics.Agents.Set(agentID, monitorData)
s.monitorLatestCache.Set(monitorID, latestMetrics, 5*time.Minute)
```

两个探针同时第一次上报同一监控项时，会各自 new 一份 map，后写覆盖先写，部分探针结果丢失。`UpdatedAt` 也是无锁字段。

**建议：** 按 `monitorID` 使用 singleflight / keyed mutex；或把 `monitorLatestCache` 换成「外层 Cache + 内层已存在则复用」的原子 GetOrSet。`LatestMonitorMetrics` 的标量字段也要纳入锁或改成 atomic。

### 3.3 探针侧：无 recover 的 goroutine + ICMP 二次 `Run`

```go
case protocol.MessageTypeMonitorConfig:
    go a.handleMonitorConfig(msg.Data)
```

普通指标采集有 panic recover，监控路径没有。Go 中未捕获 panic 会让 **整个探针进程退出**。

ICMP Unix 实现还在同一 `Pinger` 上失败后立刻再 `Run()`：

```go
if err := pinger.Run(); err != nil {
    pinger.SetPrivileged(true)
    if err := pinger.Run(); err != nil { // 第一次 Run 已 Stop/close(done) 时可能再崩
        return nil, err
    }
}
```

`pro-bing` 第一次 `Run()` 一旦进入 `run()` 并 `Stop()`，`done` 会被 close。再对同一实例 `Run()` 存在 close/send on closed channel 风险。

**建议：**

- `handleMonitorConfig`、`handleCommand` 等所有 `go` 出去的处理函数统一 `defer recover()`，并打堆栈。
- ICMP 失败回退时 **new 一个新的 Pinger**，不要复用。
- `NewPinger` 内部会做 DNS，必须加超时 context，避免 DNS 挂起拖死 goroutine。
- 限制 `Count`、`Timeout` 上限（例如 count ≤ 4，timeout ≤ 5s）。
- 全局 ICMP 并发度限制（建议 2~8），Windows 尤其要限制 `ping.exe` 进程数。

### 3.4 HTTP 检测可 OOM / 打满 fd

当前 HTTP 客户端：

- `DisableKeepAlives: true`，每次检测新建 TCP/TLS
- 默认超时 60s，配置值没有上限
- `ExpectedContent` 时 `io.ReadAll(resp.Body)` 无大小限制
- 无 `MaxConnsPerHost`、`ResponseHeaderTimeout`、`TLSHandshakeTimeout`
- 共享一个 `http.Client`，但检测是每个配置一条 goroutine，任务一多就是无界并发

**建议：**

```go
resp.Body = http.MaxBytesReader(nil, resp.Body, 1<<20) // 1MB 足够做内容匹配
io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
```

同时：

- 超时默认改为 5~10s，硬限制最大 15s。
- Transport 打开有限 Keep-Alive，设置 `MaxConnsPerHost`、`IdleConnTimeout`。
- 用信号量限制进程内同时进行的 HTTP/TCP/ICMP 检测数。
- 只允许 `GET/HEAD/POST`；拒绝任意 method。
- 内容匹配用流式 `bytes.Contains` 扫描，不要把整包读进内存。

### 3.5 后台 goroutine 都没有 Recover

以下路径 panic 会杀死进程或永久停掉后台循环：

| 路径 | 现状 |
| --- | --- |
| `MonitorScheduler` cron job | `cron.New()` 未加 `cron.Recover`、`SkipIfStillRunning` |
| `startMetricsMonitoring` | 无 recover，崩一次告警循环永远停 |
| `bestEffortProcessLoop` / `reliableProcessLoop` | 无 recover |
| `collectOnce` 超时后采集 goroutine 仍在跑 | 泄漏，下一 tick 再开一条 |

**建议：** cron 使用：

```go
cron.New(
    cron.WithSeconds(),
    cron.WithChain(cron.Recover(cron.DefaultLogger), cron.SkipIfStillRunning(cron.DefaultLogger)),
)
```

WebSocket worker、告警 ticker、采集超时 goroutine 一律 recover，并给采集增加可取消 context，超时后真正取消而不是只是不等待。

---

## 4. P1：改架构，把复杂度从「任务 × 探针」降下来

这是高并发的根因。现在服务端既当调度器又当消息广播器，探针只是无状态执行器。

### 4.1 目标模型：配置下发 + 本地调度 + 结果上报

```text
服务端                         探针
─────                         ────
监控任务变更 ──config──► 本地任务表
探针上线    ──全量config──► 按 interval 调度
                              │
                              ├─ worker pool 执行 HTTP/TCP/ICMP
                              └─ best-effort metrics 上报
结果 ──seq=0──► 聚合缓存 / VM
告警周期读取缓存，不再每次打全量库
```

协议上 `MonitorConfigPayload.Interval` 已经存在，但服务端下发时写死 `0`，探针也忽略它，只执行一次。应该真正用起来。

**下发时机只保留：**

1. 探针注册成功后下发该探针应执行的全量监控列表
2. 监控任务创建 / 更新 / 删除 / 启停时，只通知受影响探针
3. 定期对账（例如 5~10 分钟一次全量校准，防止漏配置）

**不要**每个 interval 都对所有探针广播「请立刻检测」。

收益：

- WebSocket 控制面消息从 `O(任务 × 探针 × 每分钟触发次数)` 降到 `O(配置变更)`
- 探针按自己的 interval 错峰执行，不再被服务端 cron 同时打满
- 服务端 CPU/锁/发送队列与探针数量近似线性，而不是乘积

### 4.2 结果必须走 best-effort，不要进可靠 outbox

现在 `handleMonitorConfig` 调用 `sendOutboundMessage()`，监控结果被分配 seq，和服务端安全事件、SSH 登录、防篡改共用可靠队列。

后果：

- 监控结果会堵塞 `reliableProcessLoop`
- 可靠队列 512 满了会断开连接、触发重放
- outbox 上限 1024，高频监控会挤掉真正需要可靠投递的安全事件
- 服务端对 latest-wins 的遥测做 at-least-once，纯属浪费

**建议：** 监控结果与 CPU/内存一样走 `metricsStore` 或独立的 best-effort 发送（`seq = 0`）。同一 `(monitor_id, agent_id)` 只保留最新一条。

### 4.3 探针侧合并配置，不要一条消息一个任务

即使暂时保留服务端触发，也应：

- 同一 tick 内把发往同一探针的多个监控项 **合并成一条** `Items: [...]`
- 探针用 worker pool 并行检测，而不是每条消息一个 goroutine
- 相同 `monitor_id` 若上一次还没结束，跳过本轮（in-flight dedupe），避免检测重叠

---

## 5. P2：探针执行层

`Collect()` 目前对 `Items` **完全串行**。单条消息现在通常只有 1 个 item，所以真正的并发来自「很多 goroutine 同时 Collect 一个 item」。两个方向都不好：串行会拉高单轮耗时，无界 goroutine 会打爆 fd / ICMP。

**建议实现一个 MonitorRunner：**

```text
配置表（monitorID → item+interval+nextRun）
        │
        ▼
ticker 1s 找出到期任务
        │
        ▼
worker pool（默认 8，可配）
        │
        ├─ HTTP checker（共享 Transport，有限连接）
        ├─ TCP checker（Dialer + 超时）
        └─ ICMP checker（独立 Pinger，全局信号量）
        │
        ▼
结果写入 latest map，由 sendLoop 批量上报
```

补充约束：

- 每个检测带独立 context，超时即取消。
- HTTP 默认超时 5s，TCP 3s，ICMP 总超时 3s、count 默认 1（监控存活不需要 4 次 ping）。
- 错开首次执行：`nextRun = now + hash(agentID+monitorID)%interval`，避免所有探针整点齐射同一目标。
- 日志从每条 Info 改为 debug / 失败才 Warn，避免 200 探针把磁盘打满。
- `collectOnce` 超时后必须取消子采集，否则 goroutine 会在 30s 超时后继续泄漏。

ICMP 额外注意：

- Linux 非特权 UDP ping 并发时容易互相干扰，应用进程级限流。
- 解析目标已经做了字符白名单，保持；Windows 继续走 `ping.exe`，但必须限制并发和 `count`。
- 不要在超时后残留 `ping.exe`；`CommandContext` 已经有超时，确认 kill 生效。

---

## 6. P3：服务端调度与下发

### 6.1 停止串行广播

`SendMonitorTaskToAgents` 现在对每个探针：

1. 再 marshal 一次 JSON（其实可以共用）
2. `SendToClient` 最多阻塞 3 秒

200 个慢客户端就是最多 10 分钟。cron 默认允许任务重叠，于是同一监控项会有多轮下发同时进行。

**建议：**

- cron 加 `SkipIfStillRunning`
- 下发改成 worker pool（例如 32/64），超时失败记指标，不要阻塞调度循环
- 同一 payload 只 marshal 一次
- 未连接探针直接跳过，不要 3 秒超时空等
- 目标解析结果做短 TTL 缓存，避免每个 tick 都 `FindIDsByTags` / `GetAllClients`

### 6.2 不要把监控任务打到已禁用探针

`GetAllClients()` 返回所有 WebSocket 连接，包括 disabled。禁用探针仍会执行检测，服务端再在 `handleWebSocketMessage` 里丢弃结果。

应在下发前过滤 `Enabled=true` 且当前连接为 current 的探针。

### 6.3 调度器与任务状态

- 任务更新已经用同一把锁，这点保留。
- `LoadTasks` 目前只在 `Start()` 调一次，进程外改库不会收敛。若保留服务端调度，需要定期 reconcile。
- 改为本地调度后，服务端 cron 可以降级成「配置对账」，间隔放大到分钟级。

---

## 7. P4：上报、缓存、VM、告警

### 7.1 每条消息处理不要长时间持锁

```go
func (h *AgentHandler) handleWebSocketMessage(...) error {
    h.enabledMu.RLock()
    defer h.enabledMu.RUnlock()
    // 这里会走到 HandleMetricData → VM HTTP → 可能还更新流量表
}
```

一次 Disable/Enable 要抢 write lock，会卡住 **所有探针** 的消息处理。`IsAgentEnabled` 已有 3s 缓存，这里的全局 RWMutex 没有必要包住整段 IO。

`DoIfCurrent` 同样在持有 Manager 读锁时写库。pong 是同步做的，DB 慢会卡住该连接的 `ReadPump`，并阻塞需要写锁的 `Register`。

**建议：**

- `enabledMu` 只保护状态翻转的临界区，不要包 VM/DB。
- pong 状态刷新改为：先判断 ShouldWriteStatus，再异步/带超时写库，写成功后 `DoIfCurrent` 只做内存标记。
- Manager 锁内只碰 map，IO 一律锁外。

### 7.2 指标写入合并

`HandleMetricData` 对 batch 里每个 metric type 各打一次 VM HTTP。200 探针 × 每秒一批 × 8 类指标，就是每秒上千次 `/api/v1/import`。监控结果再叠加一轮。

best-effort 队列只有 128，VM 一抖就会开始丢遥测。

**建议：**

- 进程内做 write buffer：按 200~500ms 或 N 条 flush 一次
- 监控结果批量写入，不要一条监控一次 HTTP
- `vmClient` Transport 显式设置 `MaxIdleConns`、`MaxConnsPerHost`
- 给 best-effort 处理增加超时；超时丢弃本批，不要堵住后续
- 监控缓存 5 分钟 TTL 过短，列表会闪 unknown；改为跟随任务生命周期删除，或至少 2~3 个 interval

### 7.3 列表和告警的 N+1

`ListByAuth` / `GetMonitorStats` / `GetMonitorAgentStats` / 告警检查，每个监控项都会：

1. `monitorRepo.FindById`
2. `resolveMonitorTargetSet`（可能再查标签）
3. 迭代 Agents

公开页和 30s 告警都会放大这段。

**建议：**

- 监控任务配置、目标探针集合做内存索引，配置变更时更新
- `GetAllLatestMonitorMetrics` 一次拿到全部缓存，不要每个任务打库
- 告警检查只读 snapshot，不要在检查循环里写库太多次；状态更新批量提交

### 7.4 流量统计不要跟在每秒网络指标后面同步写库

`HandleMetricData` 的 network 分支会 `trafficService.UpdateAgentTraffic`。探针多时这是每秒一次 DB 更新 × 探针数。应降频（30s/1min）或异步队列。

---

## 8. P5：连接与资源

单探针当前常驻：

- 读泵、写泵、reliable worker、best-effort worker
- send chan 512 + reliable 512 + best-effort 128
- WebSocket `ReadBufferSize/WriteBufferSize = 32KiB`，且 `EnableCompression = true`

1000 探针仅 buffer 就接近 64MB+，压缩会额外吃 CPU；压缩在 gorilla/websocket 上对小 JSON 收益很小。

**建议：**

- 关闭压缩，或仅在大包时开启
- 读/写 buffer 降到 4~8KiB
- 监控下发不要和普通 send 队列无限挤占；控制消息可用独立高优先级或 `trySend` + 下次对账补偿
- 给 Manager 的 `clients/sessions` 分片，避免一把 RWMutex 覆盖全部探针
- 为每个 session 的 `onMessage` 加 timeout context，取消时不要用「没有 deadline 的 session.ctx」去打 VM

---

## 9. 建议落地顺序

### 第一阶段（1~2 天，先止血）

1. 修复 `SafeMap` 快照迭代，监控缓存读写全部走 snapshot。
2. 探针监控/命令 goroutine 加 recover；ICMP 失败时新建 Pinger。
3. HTTP `MaxBytesReader`、超时/次数上限、检测并发信号量。
4. 监控结果改为 best-effort 上报，不再进 outbox。
5. cron 加 `Recover` + `SkipIfStillRunning`；`SendToClient` 并发化。
6. 后台循环（告警、WS worker、collectOnce）加 recover / 可取消 context。

### 第二阶段（架构，收益最大）

1. 探针本地调度：注册时下发全量配置，变更时增量更新。
2. 服务端 cron 改为对账，而不是每轮触发检测。
3. 相同探针的监控项合并下发；in-flight 去重 + 首次执行抖动。
4. 去掉 `handleWebSocketMessage` / `DoIfCurrent` 持锁做 IO。

### 第三阶段（规模化）

1. VM 写入缓冲与批量 import。
2. 监控配置 / 目标集合内存索引，去掉列表和告警 N+1。
3. WebSocket buffer / 压缩 / Manager 分片。
4. 流量统计降频。
5. 增加内部指标：在途检测数、下发失败、best-effort 丢弃、可靠队列深度、监控 panic 次数、VM 写入延迟。

---

## 10. 验收建议

可以用下面的压测口径判断优化是否生效。

**场景 A：稳定性**

- 50 个监控项（HTTP/TCP/ICMP 混合），200 个探针
- 同时打开公开服务列表轮询 + 告警检查
- 预期：服务端和探针 **0 fatal / 0 unrecovered panic**，连续运行 ≥ 1 小时

**场景 B：控制面消息量**

- 改造前：每分钟 WebSocket 下发 ≈ `任务数 × 探针数 × (60/interval)`
- 改造后：稳态每分钟控制面消息应接近 0，只在配置变更和定期对账时出现

**场景 C：数据面延迟**

- 监控结果从检测到服务端缓存可见，P99 < 2s
- best-effort 丢弃率 < 1%
- 可靠队列深度接近 0，不再因为监控结果断线重放

**场景 D：资源**

- 单探针 goroutine 不随监控项线性上涨（有 worker pool 上限）
- 服务端在 200 探针时 CPU 主要用于 VM 批量写，而不是 JSON marshal / 锁等待
- 探针打开大量 HTTP 监控时 fd 稳定，不再出现 `too many open files`

---

## 11. 不建议做的事

- 不要继续加大 `bestEffortQueueSize` / `outboxMaxEntries` 当治本方案，队列变长只会延迟崩溃。
- 不要把检测收回到服务端统一代探。当前设计的价值就是「从多个探针位置看目标」；服务端代探会失去这个语义，也会让服务端成为新的单点瓶颈。
- 不要在持有 `Manager.mu` 或 `enabledMu` 时访问数据库 / VictoriaMetrics。
- 不要对 `pro-bing.Pinger` 调用两次 `Run()`，也不要在多 goroutine 共享同一个 Pinger。

---

## 12. 最小改动对照

若只能先改几处，建议按这个补丁顺序：

1. `syncx.SafeMap` 或调用方 snapshot —— 解决服务端莫名退出
2. `pkg/agent/service/agent.go` `handleMonitorConfig` recover + best-effort 发送
3. `pkg/agent/collector/monitor.go` 超时/读限制/并发限制
4. `pkg/agent/collector/monitor_icmp_unix.go` 新建 Pinger 再 privileged 重试
5. `internal/scheduler/monitor_scheduler.go` Recover + SkipIfStillRunning
6. `internal/service/monitor_service.go` 并发下发、marshal 一次、跳过离线/禁用探针
7. 再做「本地调度」协议改造
