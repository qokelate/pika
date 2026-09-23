package collector

import (
	"strings"
	"testing"

	"github.com/pika-monitor/pika/internal/protocol"
)

func TestAllowedHTTPMethod(t *testing.T) {
	if !allowedHTTPMethod("GET") || !allowedHTTPMethod("post") {
		t.Fatal("GET/POST should be allowed")
	}
	if allowedHTTPMethod("CONNECT") || allowedHTTPMethod("TRACE") {
		t.Fatal("CONNECT/TRACE should be rejected")
	}
}

func TestClampInt(t *testing.T) {
	if got := clampInt(0, 10, 30); got != 10 {
		t.Fatalf("default = %d", got)
	}
	if got := clampInt(999, 10, 30); got != 30 {
		t.Fatalf("max = %d", got)
	}
}

func TestCheckHTTPRejectsUnsupportedMethod(t *testing.T) {
	c := NewMonitorCollector()
	denied := c.checkHTTP(protocol.MonitorItem{
		ID:     "m1",
		Type:   "http",
		Target: "http://127.0.0.1",
		HTTPConfig: &protocol.HTTPMonitorConfig{
			Method:             "CONNECT",
			ExpectedStatusCode: 200,
			Timeout:            5,
		},
	})
	if denied.Status != "down" || !strings.Contains(denied.Error, "unsupported http method") {
		t.Fatalf("CONNECT should be rejected, got %+v", denied)
	}
}

func TestCollectUnsupportedType(t *testing.T) {
	c := NewMonitorCollector()
	results := c.Collect([]protocol.MonitorItem{
		{ID: "bad", Type: "unknown", Target: "x"},
	})
	if len(results) != 1 || results[0].Status != "down" {
		t.Fatalf("unsupported type should be down: %+v", results)
	}
}
