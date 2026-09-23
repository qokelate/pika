package protocol

// MonitorConfigPayload 监控配置 payload。
// Replace=true 时用 Items 整体替换探针本地任务表；否则按 Items 增改、Removed 删除。
// Interval 仅作为旧字段兼容：Items 未带 Interval 时回落到该值，仍为 0 则视为一次性检测。
type MonitorConfigPayload struct {
	Replace  bool          `json:"replace,omitempty"`
	Removed  []string      `json:"removed,omitempty"`
	Interval int           `json:"interval,omitempty"`
	Items    []MonitorItem `json:"items"`
}

// MonitorItem 监控项配置
type MonitorItem struct {
	ID         string             `json:"id"`
	Type       string             `json:"type"`
	Target     string             `json:"target"`
	Interval   int                `json:"interval,omitempty"` // 检测间隔（秒），0 表示一次性
	HTTPConfig *HTTPMonitorConfig `json:"httpConfig,omitempty"`
	TCPConfig  *TCPMonitorConfig  `json:"tcpConfig,omitempty"`
	ICMPConfig *ICMPMonitorConfig `json:"icmpConfig,omitempty"`
}

// HTTPMonitorConfig HTTP 监控配置
type HTTPMonitorConfig struct {
	Method             string            `json:"method"`
	ExpectedStatusCode int               `json:"expectedStatusCode"`
	ExpectedContent    string            `json:"expectedContent,omitempty"`
	Timeout            int               `json:"timeout"`
	Headers            map[string]string `json:"headers,omitempty"`
	Body               string            `json:"body,omitempty"`
}

// TCPMonitorConfig TCP 监控配置
type TCPMonitorConfig struct {
	Timeout int `json:"timeout"`
}

// ICMPMonitorConfig ICMP 监控配置
type ICMPMonitorConfig struct {
	Timeout int `json:"timeout"` // 超时时间（秒）
	Count   int `json:"count"`   // Ping 次数
}
