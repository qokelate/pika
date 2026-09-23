//go:build !windows

package collector

import (
	"context"
	"time"

	probing "github.com/prometheus-community/pro-bing"
)

func pingHost(ctx context.Context, target string, count, timeoutSec int) (*pingStats, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	stats, err := runPinger(ctx, target, count, timeoutSec, false)
	if err == nil {
		return stats, nil
	}
	if ctx.Err() != nil {
		return nil, err
	}
	return runPinger(ctx, target, count, timeoutSec, true)
}

func runPinger(ctx context.Context, target string, count, timeoutSec int, privileged bool) (*pingStats, error) {
	pinger, err := probing.NewPinger(target)
	if err != nil {
		return nil, err
	}

	pinger.Count = count
	pinger.Timeout = time.Duration(timeoutSec) * time.Second
	pinger.Interval = 200 * time.Millisecond
	pinger.SetPrivileged(privileged)

	if err := pinger.RunWithContext(ctx); err != nil {
		return nil, err
	}

	s := pinger.Statistics()
	return &pingStats{
		PacketsSent: s.PacketsSent,
		PacketsRecv: s.PacketsRecv,
		AvgRttMs:    s.AvgRtt.Milliseconds(),
		PacketLoss:  s.PacketLoss,
	}, nil
}
