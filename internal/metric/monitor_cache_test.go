package metric

import (
	"sync"
	"testing"

	"github.com/pika-monitor/pika/internal/protocol"
)

func TestLatestMonitorMetricsConcurrentSnapshot(t *testing.T) {
	cache := NewLatestMonitorMetrics("m1")
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			cache.SetAgent("agent", protocol.MonitorData{
				AgentId:      "agent",
				MonitorId:    "m1",
				Status:       "up",
				ResponseTime: int64(i),
			}, int64(i))
		}(i)
		go func() {
			defer wg.Done()
			_ = cache.Snapshot()
			_ = cache.AgentIDs()
			_ = cache.Len()
		}()
	}
	wg.Wait()

	if cache.Len() != 1 {
		t.Fatalf("Len() = %d, want 1", cache.Len())
	}
	snapshot := cache.Snapshot()
	if len(snapshot) != 1 || snapshot[0].AgentId != "agent" {
		t.Fatalf("snapshot = %+v", snapshot)
	}
}
