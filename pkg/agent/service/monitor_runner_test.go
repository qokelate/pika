package service

import (
	"testing"
	"time"

	"github.com/pika-monitor/pika/internal/protocol"
)

func TestMonitorRunnerReplaceAndOneShot(t *testing.T) {
	r := newMonitorRunner()
	now := time.Unix(1700000000, 0)

	oneShot := r.apply(protocol.MonitorConfigPayload{
		Items: []protocol.MonitorItem{{ID: "once", Type: "http", Target: "https://example.com"}},
	}, now, "agent-1")
	if len(oneShot) != 1 || oneShot[0].ID != "once" {
		t.Fatalf("one-shot items = %+v", oneShot)
	}
	if r.len() != 0 {
		t.Fatalf("one-shot item should not be scheduled, got %d", r.len())
	}

	r.apply(protocol.MonitorConfigPayload{
		Replace: true,
		Items: []protocol.MonitorItem{
			{ID: "http-1", Type: "http", Target: "https://a.example", Interval: 30},
			{ID: "tcp-1", Type: "tcp", Target: "1.1.1.1:53", Interval: 60},
		},
	}, now, "agent-1")
	if r.len() != 2 {
		t.Fatalf("len after replace = %d, want 2", r.len())
	}

	r.apply(protocol.MonitorConfigPayload{
		Replace: true,
		Items:   []protocol.MonitorItem{{ID: "http-1", Type: "http", Target: "https://a.example", Interval: 30}},
	}, now, "agent-1")
	if r.len() != 1 {
		t.Fatalf("len after shrink replace = %d, want 1", r.len())
	}

	due := r.due(now.Add(2 * time.Second))
	if len(due) != 1 || due[0].ID != "http-1" {
		t.Fatalf("due = %+v", due)
	}
	if again := r.due(now.Add(2 * time.Second)); len(again) != 0 {
		t.Fatalf("in-flight item was scheduled again: %+v", again)
	}
	r.finish("http-1")
}

func TestMonitorRunnerRemoved(t *testing.T) {
	r := newMonitorRunner()
	now := time.Now()
	r.apply(protocol.MonitorConfigPayload{
		Items: []protocol.MonitorItem{{ID: "m1", Type: "icmp", Target: "1.1.1.1", Interval: 15}},
	}, now, "agent")
	r.apply(protocol.MonitorConfigPayload{Removed: []string{"m1"}}, now, "agent")
	if r.len() != 0 {
		t.Fatalf("removed item still scheduled")
	}
}
