package service

import (
	"testing"

	"github.com/pika-monitor/pika/internal/protocol"
)

func metricSample(metricType protocol.MetricType, timestamp int64) protocol.MetricSample {
	return protocol.MetricSample{Type: metricType, Timestamp: timestamp}
}

func TestMetricsStorePendingRequiresAck(t *testing.T) {
	store := newMetricsStore()
	store.put([]protocol.MetricSample{metricSample(protocol.MetricTypeCPU, 100)})

	first, cursor := store.pending()
	second, _ := store.pending()
	if len(first) != 1 || len(second) != 1 {
		t.Fatalf("unacknowledged sample must remain pending: first=%d second=%d", len(first), len(second))
	}

	store.ack(cursor)
	afterAck, _ := store.pending()
	if len(afterAck) != 0 {
		t.Fatalf("acknowledged sample returned again: %v", afterAck)
	}
}

func TestMetricsStoreAckDoesNotConsumeConcurrentUpdate(t *testing.T) {
	store := newMetricsStore()
	store.put([]protocol.MetricSample{metricSample(protocol.MetricTypeCPU, 100)})
	_, cursor := store.pending()

	store.put([]protocol.MetricSample{metricSample(protocol.MetricTypeMemory, 100)})
	store.ack(cursor)

	pending, _ := store.pending()
	if len(pending) != 1 || pending[0].Type != protocol.MetricTypeMemory {
		t.Fatalf("concurrent update was consumed by an older ack: %v", pending)
	}
}

func TestMetricsStoreUsesSequenceInsteadOfTimestampCursor(t *testing.T) {
	store := newMetricsStore()
	store.put([]protocol.MetricSample{metricSample(protocol.MetricTypeCPU, 100)})
	_, cursor := store.pending()
	store.ack(cursor)

	store.put([]protocol.MetricSample{metricSample(protocol.MetricTypeMemory, 100)})
	pending, _ := store.pending()
	if len(pending) != 1 || pending[0].Type != protocol.MetricTypeMemory {
		t.Fatalf("sample sharing an acknowledged timestamp was lost: %v", pending)
	}
}

func TestMetricsStoreIgnoresAckFromPreviousGeneration(t *testing.T) {
	store := newMetricsStore()
	store.put([]protocol.MetricSample{metricSample(protocol.MetricTypeCPU, 100)})
	_, oldCursor := store.pending()

	store.reset()
	store.ack(oldCursor)

	pending, _ := store.pending()
	if len(pending) != 1 {
		t.Fatalf("old connection ack consumed reset snapshot: %v", pending)
	}
}

func TestMetricsStoreLastInsertedSampleWinsWithinBatch(t *testing.T) {
	store := newMetricsStore()
	store.put([]protocol.MetricSample{
		metricSample(protocol.MetricTypeCPU, 200),
		metricSample(protocol.MetricTypeCPU, 100),
	})

	pending, _ := store.pending()
	if len(pending) != 1 || pending[0].Timestamp != 100 {
		t.Fatalf("last inserted sample did not win: %v", pending)
	}
}

func TestMetricsStoreAcceptsUpdateAfterClockMovesBackward(t *testing.T) {
	store := newMetricsStore()
	store.put([]protocol.MetricSample{metricSample(protocol.MetricTypeCPU, 200)})
	_, cursor := store.pending()
	store.ack(cursor)

	store.put([]protocol.MetricSample{metricSample(protocol.MetricTypeCPU, 100)})
	pending, _ := store.pending()
	if len(pending) != 1 || pending[0].Timestamp != 100 {
		t.Fatalf("sample collected after clock moved backward was lost: %v", pending)
	}
}

func TestMetricsStoreMergesMonitorResultsByID(t *testing.T) {
	store := newMetricsStore()
	store.put([]protocol.MetricSample{{
		Type: protocol.MetricTypeMonitor,
		Data: []protocol.MonitorData{{MonitorId: "m1", Status: "up", ResponseTime: 10}},
	}})
	store.put([]protocol.MetricSample{{
		Type: protocol.MetricTypeMonitor,
		Data: []protocol.MonitorData{{MonitorId: "m2", Status: "down", ResponseTime: 20}},
	}})
	store.put([]protocol.MetricSample{{
		Type: protocol.MetricTypeMonitor,
		Data: []protocol.MonitorData{{MonitorId: "m1", Status: "down", ResponseTime: 30}},
	}})

	pending, _ := store.pending()
	if len(pending) != 1 || pending[0].Type != protocol.MetricTypeMonitor {
		t.Fatalf("pending = %+v", pending)
	}
	items, ok := pending[0].Data.([]protocol.MonitorData)
	if !ok {
		t.Fatalf("data type %T", pending[0].Data)
	}
	got := map[string]protocol.MonitorData{}
	for _, item := range items {
		got[item.MonitorId] = item
	}
	if got["m1"].Status != "down" || got["m1"].ResponseTime != 30 {
		t.Fatalf("m1 not updated: %+v", got["m1"])
	}
	if got["m2"].Status != "down" {
		t.Fatalf("m2 lost: %+v", got["m2"])
	}
}
