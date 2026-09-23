package collector

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/pika-monitor/pika/internal/protocol"
)

const (
	maxConcurrentChecks = 8
	maxConcurrentICMP   = 4
	maxResponseBytes    = 1 << 20

	defaultHTTPTimeoutSec = 10
	maxHTTPTimeoutSec     = 30
	defaultTCPTimeoutSec  = 5
	maxTCPTimeoutSec      = 15
	defaultICMPTimeoutSec = 3
	maxICMPTimeoutSec     = 5
	defaultICMPCount      = 1
	maxICMPCount          = 4
)

// MonitorCollector 监控采集器
type MonitorCollector struct {
	httpClient *http.Client
	sem        chan struct{}
	icmpSem    chan struct{}
}

// NewMonitorCollector 创建监控采集器
func NewMonitorCollector() *MonitorCollector {
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   5 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true, // 允许自签名证书
		},
		TLSHandshakeTimeout:   5 * time.Second,
		ResponseHeaderTimeout: 10 * time.Second,
		IdleConnTimeout:       30 * time.Second,
		MaxIdleConns:          32,
		MaxConnsPerHost:       16,
		ForceAttemptHTTP2:     true,
	}

	return &MonitorCollector{
		httpClient: &http.Client{
			Transport: transport,
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				if len(via) >= 10 {
					return fmt.Errorf("stopped after 10 redirects")
				}
				return nil
			},
		},
		sem:     make(chan struct{}, maxConcurrentChecks),
		icmpSem: make(chan struct{}, maxConcurrentICMP),
	}
}

// Collect 采集所有监控项数据。条目并行执行，受全局并发上限约束。
func (c *MonitorCollector) Collect(items []protocol.MonitorItem) []protocol.MonitorData {
	if len(items) == 0 {
		return nil
	}

	results := make([]protocol.MonitorData, len(items))
	var wg sync.WaitGroup
	for i, item := range items {
		wg.Add(1)
		go func(i int, item protocol.MonitorItem) {
			defer wg.Done()
			c.sem <- struct{}{}
			defer func() { <-c.sem }()
			defer func() {
				if r := recover(); r != nil {
					results[i] = protocol.MonitorData{
						MonitorId: item.ID,
						Type:      item.Type,
						Target:    item.Target,
						Status:    "down",
						Error:     fmt.Sprintf("probe panic: %v", r),
						CheckedAt: time.Now().UnixMilli(),
					}
				}
			}()
			results[i] = c.checkOne(item)
		}(i, item)
	}
	wg.Wait()
	return results
}

func (c *MonitorCollector) checkOne(item protocol.MonitorItem) protocol.MonitorData {
	switch strings.ToLower(item.Type) {
	case "http", "https":
		return c.checkHTTP(item)
	case "tcp":
		return c.checkTCP(item)
	case "icmp", "ping":
		return c.checkICMP(item)
	default:
		return protocol.MonitorData{
			MonitorId: item.ID,
			Type:      item.Type,
			Target:    item.Target,
			Status:    "down",
			Error:     fmt.Sprintf("unsupported monitor type: %s", item.Type),
			CheckedAt: time.Now().UnixMilli(),
		}
	}
}

func clampInt(v, def, max int) int {
	if v <= 0 {
		v = def
	}
	if v > max {
		v = max
	}
	return v
}

func allowedHTTPMethod(method string) bool {
	switch strings.ToUpper(method) {
	case http.MethodGet, http.MethodHead, http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete, http.MethodOptions:
		return true
	default:
		return false
	}
}

// checkHTTP 检查 HTTP/HTTPS 服务
func (c *MonitorCollector) checkHTTP(item protocol.MonitorItem) protocol.MonitorData {
	result := protocol.MonitorData{
		MonitorId: item.ID,
		Type:      item.Type,
		Target:    item.Target,
		CheckedAt: time.Now().UnixMilli(),
	}

	httpCfg := item.HTTPConfig
	if httpCfg == nil {
		httpCfg = &protocol.HTTPMonitorConfig{
			Method:             "GET",
			ExpectedStatusCode: 200,
			Timeout:            defaultHTTPTimeoutSec,
		}
	}

	method := httpCfg.Method
	if method == "" {
		method = http.MethodGet
	}
	if !allowedHTTPMethod(method) {
		result.Status = "down"
		result.Error = fmt.Sprintf("unsupported http method: %s", method)
		return result
	}

	timeout := clampInt(httpCfg.Timeout, defaultHTTPTimeoutSec, maxHTTPTimeoutSec)
	expectedStatus := httpCfg.ExpectedStatusCode
	if expectedStatus == 0 {
		expectedStatus = 200
	}

	var bodyReader io.Reader
	if httpCfg.Body != "" {
		bodyReader = strings.NewReader(httpCfg.Body)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, strings.ToUpper(method), item.Target, bodyReader)
	if err != nil {
		result.Status = "down"
		result.Error = fmt.Sprintf("create request failed: %v", err)
		return result
	}

	if httpCfg.Headers != nil {
		for key, value := range httpCfg.Headers {
			req.Header.Set(key, value)
		}
	}

	startTime := time.Now()
	resp, err := c.httpClient.Do(req)
	result.ResponseTime = time.Since(startTime).Milliseconds()
	if err != nil {
		result.Status = "down"
		result.Error = fmt.Sprintf("request failed: %v", err)
		return result
	}
	defer func() {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxResponseBytes))
		_ = resp.Body.Close()
	}()

	result.StatusCode = resp.StatusCode
	if resp.StatusCode != expectedStatus {
		result.Status = "down"
		result.Error = fmt.Sprintf("status code mismatch: expected %d, got %d", expectedStatus, resp.StatusCode)
		result.Message = fmt.Sprintf("HTTP %d", resp.StatusCode)
		return result
	}

	if httpCfg.ExpectedContent != "" {
		limited := http.MaxBytesReader(nil, resp.Body, maxResponseBytes)
		body, err := io.ReadAll(limited)
		if err != nil {
			result.Status = "down"
			result.Error = fmt.Sprintf("read response body failed: %v", err)
			return result
		}
		if !bytes.Contains(body, []byte(httpCfg.ExpectedContent)) {
			result.Status = "down"
			result.Error = fmt.Sprintf("content does not contain expected string: %s", httpCfg.ExpectedContent)
			result.ContentMatch = false
			return result
		}
		result.ContentMatch = true
	}

	if resp.TLS != nil && len(resp.TLS.PeerCertificates) > 0 {
		cert := resp.TLS.PeerCertificates[0]
		expiryTime := cert.NotAfter
		result.CertExpiryTime = expiryTime.UnixMilli()
		result.CertDaysLeft = int(time.Until(expiryTime).Hours() / 24)
	}

	result.Status = "up"
	result.Message = fmt.Sprintf("HTTP %d - %dms", resp.StatusCode, result.ResponseTime)
	return result
}

// checkTCP 检查 TCP 端口
func (c *MonitorCollector) checkTCP(item protocol.MonitorItem) protocol.MonitorData {
	result := protocol.MonitorData{
		MonitorId: item.ID,
		Type:      item.Type,
		Target:    item.Target,
		CheckedAt: time.Now().UnixMilli(),
	}

	timeout := defaultTCPTimeoutSec
	if item.TCPConfig != nil {
		timeout = clampInt(item.TCPConfig.Timeout, defaultTCPTimeoutSec, maxTCPTimeoutSec)
	} else {
		timeout = clampInt(0, defaultTCPTimeoutSec, maxTCPTimeoutSec)
	}

	startTime := time.Now()
	conn, err := net.DialTimeout("tcp", item.Target, time.Duration(timeout)*time.Second)
	result.ResponseTime = time.Since(startTime).Milliseconds()
	if err != nil {
		result.Status = "down"
		result.Error = fmt.Sprintf("connection failed: %v", err)
		return result
	}
	_ = conn.Close()

	result.Status = "up"
	result.Message = fmt.Sprintf("TCP connected - %dms", result.ResponseTime)
	return result
}

// checkICMP 检查 ICMP (Ping)
func (c *MonitorCollector) checkICMP(item protocol.MonitorItem) protocol.MonitorData {
	result := protocol.MonitorData{
		MonitorId: item.ID,
		Type:      item.Type,
		Target:    item.Target,
		CheckedAt: time.Now().UnixMilli(),
	}

	timeout := defaultICMPTimeoutSec
	count := defaultICMPCount
	if item.ICMPConfig != nil {
		timeout = clampInt(item.ICMPConfig.Timeout, defaultICMPTimeoutSec, maxICMPTimeoutSec)
		count = clampInt(item.ICMPConfig.Count, defaultICMPCount, maxICMPCount)
	}

	if !isValidPingTarget(item.Target) {
		result.Status = "down"
		result.Error = fmt.Sprintf("invalid ping target: %s", item.Target)
		return result
	}

	c.icmpSem <- struct{}{}
	defer func() { <-c.icmpSem }()

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
	defer cancel()

	stats, err := pingHost(ctx, item.Target, count, timeout)
	if err != nil {
		result.Status = "down"
		result.Error = fmt.Sprintf("ping failed: %v", err)
		return result
	}

	if stats.PacketsRecv > 0 {
		result.Status = "up"
		result.ResponseTime = stats.AvgRttMs
		loss := int(stats.PacketLoss)
		result.Message = fmt.Sprintf("ICMP Echo Reply - %d/%d packets, %dms avg, %d%% loss",
			stats.PacketsRecv, stats.PacketsSent, stats.AvgRttMs, loss)
	} else {
		result.Status = "down"
		result.Error = fmt.Sprintf("all %d ping attempts failed (timeout: %ds)", count, timeout)
		result.Message = "100% packet loss"
	}

	return result
}

type pingStats struct {
	PacketsSent int
	PacketsRecv int
	AvgRttMs    int64
	PacketLoss  float64
}

func isValidPingTarget(target string) bool {
	if target == "" || strings.HasPrefix(target, "-") {
		return false
	}
	for _, r := range target {
		switch {
		case r >= 'a' && r <= 'z':
		case r >= 'A' && r <= 'Z':
		case r >= '0' && r <= '9':
		case r == '.', r == ':', r == '-', r == '_':
		default:
			return false
		}
	}
	return true
}
