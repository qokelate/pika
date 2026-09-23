package service

import (
	"hash/fnv"
	"sync"
	"time"

	"github.com/pika-monitor/pika/internal/protocol"
)

const (
	monitorWorkerLimit = 8
	monitorMaxInterval = 24 * time.Hour
)

type scheduledMonitor struct {
	item     protocol.MonitorItem
	interval time.Duration
	nextRun  time.Time
}

// monitorRunner 在探针本地调度服务监控任务，避免服务端每个 interval 广播触发。
type monitorRunner struct {
	mu       sync.Mutex
	items    map[string]*scheduledMonitor
	inflight map[string]struct{}
	workers  chan struct{}
}

func newMonitorRunner() *monitorRunner {
	return &monitorRunner{
		items:    make(map[string]*scheduledMonitor),
		inflight: make(map[string]struct{}),
		workers:  make(chan struct{}, monitorWorkerLimit),
	}
}

func monitorInterval(item protocol.MonitorItem, payloadInterval int) time.Duration {
	sec := item.Interval
	if sec <= 0 {
		sec = payloadInterval
	}
	if sec <= 0 {
		return 0
	}
	d := time.Duration(sec) * time.Second
	if d > monitorMaxInterval {
		return monitorMaxInterval
	}
	return d
}

func jitterDuration(key, monitorID string, interval time.Duration) time.Duration {
	if interval <= 0 {
		return 0
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(key))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(monitorID))
	return time.Duration(h.Sum64() % uint64(interval))
}

func (r *monitorRunner) apply(payload protocol.MonitorConfigPayload, now time.Time, jitterKey string) []protocol.MonitorItem {
	r.mu.Lock()
	defer r.mu.Unlock()

	if payload.Replace {
		keep := make(map[string]struct{}, len(payload.Items))
		for _, item := range payload.Items {
			if item.ID != "" {
				keep[item.ID] = struct{}{}
			}
		}
		for id := range r.items {
			if _, ok := keep[id]; !ok {
				delete(r.items, id)
			}
		}
	}

	for _, id := range payload.Removed {
		delete(r.items, id)
	}

	var oneShot []protocol.MonitorItem
	for _, item := range payload.Items {
		if item.ID == "" {
			continue
		}
		interval := monitorInterval(item, payload.Interval)
		if interval == 0 {
			delete(r.items, item.ID)
			if _, busy := r.inflight[item.ID]; !busy {
				r.inflight[item.ID] = struct{}{}
				oneShot = append(oneShot, item)
			}
			continue
		}

		// 配置变更后尽快执行一次，再用 interval 的短抖动避免齐射。
		r.items[item.ID] = &scheduledMonitor{
			item:     item,
			interval: interval,
			nextRun:  now.Add(jitterDuration(jitterKey, item.ID, min(interval, time.Second))),
		}
	}
	return oneShot
}

func (r *monitorRunner) due(now time.Time) []protocol.MonitorItem {
	r.mu.Lock()
	defer r.mu.Unlock()

	var items []protocol.MonitorItem
	for id, sch := range r.items {
		if _, busy := r.inflight[id]; busy {
			continue
		}
		if now.Before(sch.nextRun) {
			continue
		}
		items = append(items, sch.item)
		r.inflight[id] = struct{}{}
		sch.nextRun = now.Add(sch.interval)
	}
	return items
}

func (r *monitorRunner) finish(id string) {
	r.mu.Lock()
	delete(r.inflight, id)
	r.mu.Unlock()
}

func (r *monitorRunner) acquire() {
	r.workers <- struct{}{}
}

func (r *monitorRunner) release() {
	<-r.workers
}

func (r *monitorRunner) len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.items)
}
