package vmclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"time"
)

const (
	httpMaxIdleConns        = 128
	httpMaxIdleConnsPerHost = 32
	httpMaxConnsPerHost     = 32

	writeBatchMaxMetrics = 2000
	writeBatchMaxWait    = 50 * time.Millisecond
	writeQueueSize       = 1024
	writeWorkers         = 4
	maxResponseBody      = 8 << 20
)

var errClientClosed = errors.New("victoria metrics client closed")

// VMClient VictoriaMetrics 客户端
type VMClient struct {
	baseURL      string
	httpClient   *http.Client
	writeTimeout time.Duration
	queryTimeout time.Duration

	writeCh   chan writeRequest
	stopCh    chan struct{}
	stopped   atomic.Bool
	writeOnce sync.Once
	wg        sync.WaitGroup
}

type writeRequest struct {
	metrics []Metric
	result  chan error
}

// QueryResult 查询结果
type QueryResult struct {
	Status string     `json:"status"`
	Data   ResultData `json:"data"`
}

// ResultData 查询结果数据
type ResultData struct {
	ResultType string   `json:"resultType"`
	Result     []Result `json:"result"`
}

// Result 单个时间序列结果
type Result struct {
	Metric map[string]string `json:"metric"`
	Values [][]interface{}   `json:"values"` // [[timestamp, value], ...]
}

// DataPoint 数据点
type DataPoint struct {
	Timestamp int64
	Value     float64
	Labels    map[string]string
}

// Metric VictoriaMetrics JSON Line Format 指标
type Metric struct {
	Metric     map[string]string `json:"metric"`
	Values     []float64         `json:"values"`
	Timestamps []int64           `json:"timestamps"`
}

// NewVMClient 创建 VictoriaMetrics 客户端
func NewVMClient(baseURL string, writeTimeout, queryTimeout time.Duration) *VMClient {
	if writeTimeout == 0 {
		writeTimeout = 30 * time.Second
	}
	if queryTimeout == 0 {
		queryTimeout = 60 * time.Second
	}

	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConns = httpMaxIdleConns
	transport.MaxIdleConnsPerHost = httpMaxIdleConnsPerHost
	transport.MaxConnsPerHost = httpMaxConnsPerHost

	return &VMClient{
		baseURL: baseURL,
		httpClient: &http.Client{
			Transport: transport,
		},
		writeTimeout: writeTimeout,
		queryTimeout: queryTimeout,
		writeCh:      make(chan writeRequest, writeQueueSize),
		stopCh:       make(chan struct{}),
	}
}

// Write 写入指标（VictoriaMetrics JSON Line Format）。
// 多个并发 Write 会在短窗口内合并成一次 HTTP POST，避免 1000 探针
// 各自建连把本机 ephemeral port / conntrack 打满。
func (c *VMClient) Write(ctx context.Context, metrics []Metric) error {
	if len(metrics) == 0 {
		return nil
	}
	c.startWriter()

	result := make(chan error, 1)
	req := writeRequest{metrics: metrics, result: result}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.stopCh:
		return errClientClosed
	case c.writeCh <- req:
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-result:
		return err
	}
}

// Close 停止后台写入合并协程。生产进程无需调用；测试在 Write 完成后关闭。
func (c *VMClient) Close() {
	if c.stopped.CompareAndSwap(false, true) {
		close(c.stopCh)
	}
	c.wg.Wait()
}

func (c *VMClient) startWriter() {
	c.writeOnce.Do(func() {
		c.wg.Add(1)
		go c.runWriter()
	})
}

func (c *VMClient) runWriter() {
	defer c.wg.Done()

	sem := make(chan struct{}, writeWorkers)
	var inflight sync.WaitGroup
	defer inflight.Wait()

	var pending []writeRequest
	pendingMetrics := 0
	timer := time.NewTimer(writeBatchMaxWait)
	defer timer.Stop()
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	timerRunning := false

	stopTimer := func() {
		if !timerRunning {
			return
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timerRunning = false
	}

	flush := func() {
		if len(pending) == 0 {
			return
		}
		batch := pending
		pending = nil
		pendingMetrics = 0
		stopTimer()

		inflight.Add(1)
		sem <- struct{}{}
		go func() {
			defer func() {
				<-sem
				inflight.Done()
			}()
			c.flushBatch(batch)
		}()
	}

	for {
		select {
		case req := <-c.writeCh:
			pending = append(pending, req)
			pendingMetrics += len(req.metrics)
			if pendingMetrics >= writeBatchMaxMetrics {
				flush()
			} else if !timerRunning {
				timer.Reset(writeBatchMaxWait)
				timerRunning = true
			}
		case <-timer.C:
			timerRunning = false
			flush()
		case <-c.stopCh:
			for {
				select {
				case req := <-c.writeCh:
					pending = append(pending, req)
					pendingMetrics += len(req.metrics)
				default:
					flush()
					return
				}
			}
		}
	}
}

func (c *VMClient) flushBatch(batch []writeRequest) {
	total := 0
	for _, req := range batch {
		total += len(req.metrics)
	}
	metrics := make([]Metric, 0, total)
	for _, req := range batch {
		metrics = append(metrics, req.metrics...)
	}

	err := c.writeDirect(context.Background(), metrics)
	for _, req := range batch {
		req.result <- err
	}
}

func (c *VMClient) writeDirect(ctx context.Context, metrics []Metric) error {
	if len(metrics) == 0 {
		return nil
	}

	reqCtx, cancel := context.WithTimeout(ctx, c.writeTimeout)
	defer cancel()

	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)
	for _, metric := range metrics {
		if err := encoder.Encode(metric); err != nil {
			return fmt.Errorf("encode metric failed: %w", err)
		}
	}

	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, c.baseURL+"/api/v1/import", &buf)
	if err != nil {
		return fmt.Errorf("create request failed: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-ndjson")

	status, body, err := c.do(req)
	if err != nil {
		return fmt.Errorf("write metrics failed: %w", err)
	}
	if status != http.StatusNoContent && status != http.StatusOK {
		return fmt.Errorf("write metrics failed with status %d: %s", status, string(body))
	}
	return nil
}

func (c *VMClient) do(req *http.Request) (int, []byte, error) {
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBody))
	if err != nil {
		return resp.StatusCode, nil, err
	}
	return resp.StatusCode, body, nil
}

// AutoStep 根据查询范围自动生成 step（适用于 VictoriaMetrics）
func AutoStep(start, end time.Time) time.Duration {
	r := end.Sub(start)

	switch {
	case r <= 5*time.Minute:
		// 实时/极短窗口：与探针 1s 采集对齐，避免聚合丢点
		return 1 * time.Second
	case r <= time.Hour:
		return 10 * time.Second
	case r <= 3*time.Hour:
		return 15 * time.Second
	case r <= 6*time.Hour:
		return 30 * time.Second
	case r <= 12*time.Hour:
		return time.Minute
	case r <= 24*time.Hour:
		return 2 * time.Minute
	case r <= 3*24*time.Hour:
		return 5 * time.Minute
	case r <= 7*24*time.Hour:
		return 10 * time.Minute
	case r <= 30*24*time.Hour:
		return 30 * time.Minute
	default:
		return time.Hour
	}
}

// QueryRange 范围查询
// 如果 step 为 0，则让 VictoriaMetrics 自动选择合适的步长
func (c *VMClient) QueryRange(ctx context.Context, query string, start, end time.Time, step time.Duration) (*QueryResult, error) {
	reqCtx, cancel := context.WithTimeout(ctx, c.queryTimeout)
	defer cancel()

	params := url.Values{}
	params.Set("query", query)
	params.Set("start", fmt.Sprintf("%d", start.Unix()))
	params.Set("end", fmt.Sprintf("%d", end.Unix()))
	if step > 0 {
		params.Set("step", fmt.Sprintf("%ds", int(step.Seconds())))
	} else {
		autoStep := AutoStep(start, end)
		params.Set("step", fmt.Sprintf("%ds", int(autoStep.Seconds())))
	}

	reqURL := fmt.Sprintf("%s/api/v1/query_range?%s", c.baseURL, params.Encode())
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("create request failed: %w", err)
	}

	status, body, err := c.do(req)
	if err != nil {
		return nil, fmt.Errorf("query range failed: %w", err)
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("query range failed with status %d: %s", status, string(body))
	}

	var result QueryResult
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("decode response failed: %w", err)
	}
	if result.Status != "success" {
		return nil, fmt.Errorf("query failed with status: %s", result.Status)
	}
	return &result, nil
}

// Query 即时查询
func (c *VMClient) Query(ctx context.Context, query string) (*QueryResult, error) {
	reqCtx, cancel := context.WithTimeout(ctx, c.queryTimeout)
	defer cancel()

	params := url.Values{}
	params.Set("query", query)
	reqURL := fmt.Sprintf("%s/api/v1/query?%s", c.baseURL, params.Encode())

	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("create request failed: %w", err)
	}

	status, body, err := c.do(req)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("query failed with status %d: %s", status, string(body))
	}

	var result QueryResult
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("decode response failed: %w", err)
	}
	if result.Status != "success" {
		return nil, fmt.Errorf("query failed with status: %s", result.Status)
	}
	return &result, nil
}

// DeleteSeries 删除时间序列数据
func (c *VMClient) DeleteSeries(ctx context.Context, matchers []string) error {
	if len(matchers) == 0 {
		return nil
	}

	reqCtx, cancel := context.WithTimeout(ctx, c.queryTimeout)
	defer cancel()

	params := url.Values{}
	for _, matcher := range matchers {
		params.Add("match[]", matcher)
	}

	reqURL := fmt.Sprintf("%s/api/v1/admin/tsdb/delete_series?%s", c.baseURL, params.Encode())
	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, reqURL, nil)
	if err != nil {
		return fmt.Errorf("create request failed: %w", err)
	}

	status, body, err := c.do(req)
	if err != nil {
		return fmt.Errorf("delete series failed: %w", err)
	}
	if status != http.StatusOK && status != http.StatusNoContent {
		return fmt.Errorf("delete series failed with status %d: %s", status, string(body))
	}
	return nil
}

// ConvertToDataPoints 将查询结果转换为数据点列表
func ConvertToDataPoints(result *QueryResult) []DataPoint {
	if result == nil || len(result.Data.Result) == 0 {
		return []DataPoint{}
	}

	var points []DataPoint
	for _, r := range result.Data.Result {
		for _, v := range r.Values {
			if len(v) < 2 {
				continue
			}

			timestamp, ok := v[0].(float64)
			if !ok {
				continue
			}

			valueStr, ok := v[1].(string)
			if !ok {
				continue
			}

			var value float64
			if _, err := fmt.Sscanf(valueStr, "%f", &value); err != nil {
				continue
			}

			points = append(points, DataPoint{
				Timestamp: int64(timestamp * 1000),
				Value:     value,
				Labels:    r.Metric,
			})
		}
	}

	return points
}

// GetLabelValues 获取指定 label 的所有值
func (c *VMClient) GetLabelValues(ctx context.Context, labelName string, match []string) ([]string, error) {
	reqCtx, cancel := context.WithTimeout(ctx, c.queryTimeout)
	defer cancel()

	params := url.Values{}
	for _, m := range match {
		params.Add("match[]", m)
	}

	reqURL := fmt.Sprintf("%s/api/v1/label/%s/values?%s", c.baseURL, labelName, params.Encode())
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("create request failed: %w", err)
	}

	status, body, err := c.do(req)
	if err != nil {
		return nil, fmt.Errorf("get label values failed: %w", err)
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("get label values failed with status %d: %s", status, string(body))
	}

	var result struct {
		Status string   `json:"status"`
		Data   []string `json:"data"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("decode response failed: %w", err)
	}
	if result.Status != "success" {
		return nil, fmt.Errorf("get label values failed with status: %s", result.Status)
	}
	return result.Data, nil
}
