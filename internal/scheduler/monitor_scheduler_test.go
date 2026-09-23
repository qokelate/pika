package scheduler

import (
	"testing"

	"go.uber.org/zap"
)

func TestMonitorSchedulerKeepsSingleReconcileJob(t *testing.T) {
	scheduler := NewMonitorScheduler(nil, zap.NewNop())
	id, err := scheduler.cron.AddFunc(monitorReconcileSpec, scheduler.reconcile)
	if err != nil {
		t.Fatalf("AddFunc() error = %v", err)
	}
	scheduler.reconcileID = id

	if err := scheduler.AddTask("monitor-1", 10); err != nil {
		t.Fatalf("AddTask() error = %v", err)
	}
	if err := scheduler.UpdateTask("monitor-1", 30); err != nil {
		t.Fatalf("UpdateTask() error = %v", err)
	}
	scheduler.RemoveTask("monitor-1")
	if err := scheduler.AddTask("monitor-2", 15); err != nil {
		t.Fatalf("AddTask() error = %v", err)
	}

	entries := scheduler.cron.Entries()
	if len(entries) != 1 {
		t.Fatalf("len(cron.Entries()) = %d, want 1", len(entries))
	}
	if scheduler.GetTaskCount() != 1 {
		t.Fatalf("GetTaskCount() = %d, want 1", scheduler.GetTaskCount())
	}
}
