package service

import (
	"sort"
	"sync"

	"github.com/pika-monitor/pika/internal/protocol"
)

// metricsStore is a constant-size, latest-wins snapshot store. Delivery state
// uses an internal sequence instead of sample timestamps, which may collide or
// move backwards when the system clock changes.
type metricsStore struct {
	mu         sync.RWMutex
	latest     map[protocol.MetricType]versionedMetric
	nextSeq    uint64
	ackedSeq   uint64
	generation uint64
}

type versionedMetric struct {
	sample protocol.MetricSample
	seq    uint64
}

// metricsCursor identifies exactly the snapshot observed by pending. The
// generation prevents a delayed acknowledgement from an old connection from
// advancing the cursor after reset.
type metricsCursor struct {
	seq        uint64
	generation uint64
}

func newMetricsStore() *metricsStore {
	return &metricsStore{latest: make(map[protocol.MetricType]versionedMetric)}
}

// put stores samples in collection order. The last inserted sample for a
// metric type wins, regardless of its wall-clock timestamp.
func (s *metricsStore) put(samples []protocol.MetricSample) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, sample := range samples {
		if sample.Type == protocol.MetricTypeMonitor {
			s.mergeMonitorLocked(sample)
			continue
		}
		s.nextSeq++
		s.latest[sample.Type] = versionedMetric{sample: sample, seq: s.nextSeq}
	}
}

func (s *metricsStore) mergeMonitorLocked(sample protocol.MetricSample) {
	incoming := monitorDataFromSample(sample)
	if len(incoming) == 0 {
		return
	}

	merged := make(map[string]protocol.MonitorData)
	if existing, ok := s.latest[protocol.MetricTypeMonitor]; ok {
		for _, item := range monitorDataFromSample(existing.sample) {
			if item.MonitorId != "" {
				merged[item.MonitorId] = item
			}
		}
	}
	for _, item := range incoming {
		if item.MonitorId == "" {
			continue
		}
		merged[item.MonitorId] = item
	}

	items := make([]protocol.MonitorData, 0, len(merged))
	for _, item := range merged {
		items = append(items, item)
	}
	s.nextSeq++
	sample.Data = items
	s.latest[protocol.MetricTypeMonitor] = versionedMetric{sample: sample, seq: s.nextSeq}
}

func monitorDataFromSample(sample protocol.MetricSample) []protocol.MonitorData {
	switch data := sample.Data.(type) {
	case []protocol.MonitorData:
		return data
	case *[]protocol.MonitorData:
		if data == nil {
			return nil
		}
		return *data
	default:
		return nil
	}
}

// pending returns samples not acknowledged on the current connection. It does
// not mutate delivery state; the caller must acknowledge the cursor only after
// the complete network write succeeds.
func (s *metricsStore) pending() ([]protocol.MetricSample, metricsCursor) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	cursor := metricsCursor{seq: s.ackedSeq, generation: s.generation}
	samples := make([]protocol.MetricSample, 0, len(s.latest))
	for _, metric := range s.latest {
		if metric.seq <= s.ackedSeq {
			continue
		}
		samples = append(samples, metric.sample)
		if metric.seq > cursor.seq {
			cursor.seq = metric.seq
		}
	}

	sort.Slice(samples, func(i, j int) bool {
		if samples[i].Timestamp == samples[j].Timestamp {
			return samples[i].Type < samples[j].Type
		}
		return samples[i].Timestamp < samples[j].Timestamp
	})
	return samples, cursor
}

// ack commits a successful write. Stale and old-connection acknowledgements
// are harmless.
func (s *metricsStore) ack(cursor metricsCursor) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if cursor.generation == s.generation && cursor.seq > s.ackedSeq {
		s.ackedSeq = cursor.seq
	}
}

// reset starts a new delivery generation and makes the current full snapshot
// pending for the new connection.
func (s *metricsStore) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.generation++
	s.ackedSeq = 0
}
