package vmclient

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestHTTPClientUsesConnectionPool(t *testing.T) {
	client := NewVMClient("http://127.0.0.1:8428", time.Second, time.Second)
	t.Cleanup(client.Close)

	transport, ok := client.httpClient.Transport.(*http.Transport)
	if !ok {
		t.Fatal("expected *http.Transport")
	}
	if transport.MaxIdleConnsPerHost < 8 {
		t.Fatalf("MaxIdleConnsPerHost too small: %d", transport.MaxIdleConnsPerHost)
	}
	if transport.MaxConnsPerHost <= 0 {
		t.Fatalf("MaxConnsPerHost should cap same-host connections, got %d", transport.MaxConnsPerHost)
	}
}

func TestWriteCoalescesConcurrentRequests(t *testing.T) {
	var requests atomic.Int32
	var lines atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/import" {
			t.Errorf("unexpected path %s", r.URL.Path)
		}
		requests.Add(1)
		scanner := bufio.NewScanner(r.Body)
		for scanner.Scan() {
			if len(scanner.Bytes()) > 0 {
				lines.Add(1)
			}
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client := NewVMClient(server.URL, time.Second, time.Second)
	t.Cleanup(client.Close)

	const writers = 100
	start := make(chan struct{})
	var wg sync.WaitGroup
	errCh := make(chan error, writers)
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			errCh <- client.Write(context.Background(), []Metric{{
				Metric:     map[string]string{"__name__": "pika_cpu_usage_percent", "agent_id": "agent"},
				Values:     []float64{float64(i)},
				Timestamps: []int64{int64(i)},
			}})
		}(i)
	}
	close(start)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("Write returned error: %v", err)
		}
	}

	if got := lines.Load(); got != writers {
		t.Fatalf("expected %d metrics, got %d", writers, got)
	}
	if got := requests.Load(); got == 0 || got >= writers {
		t.Fatalf("expected coalesced VM writes, got %d requests for %d metrics", got, writers)
	}
}

func TestWriteReusesTCPConnections(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client := NewVMClient(server.URL, time.Second, time.Second)
	t.Cleanup(client.Close)

	var dials atomic.Int32
	transport := client.httpClient.Transport.(*http.Transport).Clone()
	baseDial := transport.DialContext
	if baseDial == nil {
		baseDial = (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext
	}
	transport.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		dials.Add(1)
		return baseDial(ctx, network, addr)
	}
	client.httpClient.Transport = transport

	metrics := make([]Metric, writeBatchMaxMetrics)
	for i := range metrics {
		metrics[i] = Metric{
			Metric:     map[string]string{"__name__": "pika_cpu_usage_percent", "agent_id": "agent"},
			Values:     []float64{1},
			Timestamps: []int64{int64(i)},
		}
	}
	for i := 0; i < 5; i++ {
		if err := client.Write(context.Background(), metrics); err != nil {
			t.Fatalf("Write #%d failed: %v", i, err)
		}
	}
	if got := dials.Load(); got > 2 {
		t.Fatalf("expected HTTP connection reuse, got %d dials", got)
	}
}

func TestWritePropagatesStatusError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "no space left", http.StatusInsufficientStorage)
	}))
	defer server.Close()

	client := NewVMClient(server.URL, time.Second, time.Second)
	t.Cleanup(client.Close)

	err := client.Write(context.Background(), []Metric{{
		Metric:     map[string]string{"__name__": "pika_cpu_usage_percent"},
		Values:     []float64{1},
		Timestamps: []int64{1},
	}})
	if err == nil {
		t.Fatal("expected write error")
	}
}

func TestWriteEmptyDoesNotHitServer(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client := NewVMClient(server.URL, time.Second, time.Second)
	t.Cleanup(client.Close)
	if err := client.Write(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	if requests.Load() != 0 {
		t.Fatalf("empty write should not send HTTP request")
	}
}

func TestQueryRangeReadsSuccessBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(QueryResult{Status: "success"})
	}))
	defer server.Close()

	client := NewVMClient(server.URL, time.Second, time.Second)
	t.Cleanup(client.Close)
	result, err := client.QueryRange(context.Background(), "up", time.Now().Add(-time.Minute), time.Now(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "success" {
		t.Fatalf("unexpected status %s", result.Status)
	}
}

func TestWriteEncodesNDJSON(t *testing.T) {
	var payload []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		payload, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client := NewVMClient(server.URL, time.Second, time.Second)
	t.Cleanup(client.Close)
	if err := client.Write(context.Background(), []Metric{
		{Metric: map[string]string{"__name__": "a"}, Values: []float64{1}, Timestamps: []int64{1}},
		{Metric: map[string]string{"__name__": "b"}, Values: []float64{2}, Timestamps: []int64{2}},
	}); err != nil {
		t.Fatal(err)
	}

	scanner := bufio.NewScanner(bytes.NewReader(payload))
	var count int
	for scanner.Scan() {
		count++
	}
	if count != 2 {
		t.Fatalf("expected 2 ndjson lines, got %d payload=%q", count, payload)
	}
}
