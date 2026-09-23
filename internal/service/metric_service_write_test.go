package service

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pika-monitor/pika/internal/protocol"
	"github.com/pika-monitor/pika/internal/vmclient"
	"go.uber.org/zap"
)

func TestHandleMetricsBatchWritesOnce(t *testing.T) {
	var requests atomic.Int32
	var lines atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		scanner := bufio.NewScanner(r.Body)
		for scanner.Scan() {
			if len(scanner.Bytes()) == 0 {
				continue
			}
			var metric vmclient.Metric
			if err := json.Unmarshal(scanner.Bytes(), &metric); err != nil {
				t.Errorf("invalid ndjson: %v", err)
				continue
			}
			lines.Add(1)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	vm := vmclient.NewVMClient(server.URL, time.Second, time.Second)
	t.Cleanup(vm.Close)
	svc := NewMetricService(zap.NewNop(), nil, nil, nil, vm)

	err := svc.HandleMetricsBatch(context.Background(), "agent-1", []protocol.MetricSample{
		{Type: protocol.MetricTypeCPU, Data: protocol.CPUData{UsagePercent: 12, LogicalCores: 4, PhysicalCores: 2}, Timestamp: 1000},
		{Type: protocol.MetricTypeMemory, Data: protocol.MemoryData{UsagePercent: 34, Total: 1024, Used: 256}, Timestamp: 1000},
	})
	if err != nil {
		t.Fatalf("HandleMetricsBatch returned error: %v", err)
	}

	if got := requests.Load(); got != 1 {
		t.Fatalf("expected a single VM write for the batch, got %d", got)
	}
	if got := lines.Load(); got == 0 {
		t.Fatal("expected metric lines to be written")
	}

	latest, ok := svc.GetLatestMetrics("agent-1")
	if !ok || latest.CPU == nil || latest.Memory == nil {
		t.Fatalf("expected latest cache to be updated, got ok=%v latest=%+v", ok, latest)
	}
}
