//go:build windows

package collector

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"regexp"
	"strconv"
	"syscall"
	"time"
)

// "TTL=" 在 Windows ping.exe 各语言版本输出中都保留为英文字面量，
// 因此用作回包行的锚点比 "Reply from" / "来自" 之类本地化文案更稳妥
var (
	winPingReplyAnchor = regexp.MustCompile(`(?i)TTL[=:]`)
	winPingTimeRe      = regexp.MustCompile(`(?i)[=<]\s*(\d+)\s*ms`)
)

func pingHost(ctx context.Context, target string, count, timeoutSec int) (*pingStats, error) {
	if count <= 0 {
		count = 1
	}
	if timeoutSec <= 0 {
		timeoutSec = 3
	}

	perReplyMs := timeoutSec * 1000

	// 进程级超时取每包超时 × 包数 + 余量；ping 自身在包间还会停 ~1s
	overall := time.Duration(timeoutSec*count+5) * time.Second
	ctx, cancel := context.WithTimeout(ctx, overall)
	defer cancel()

	cmd := exec.CommandContext(ctx, "ping",
		"-n", strconv.Itoa(count),
		"-w", strconv.Itoa(perReplyMs),
		target,
	)
	cmd.SysProcAttr = &syscall.SysProcAttr{
		HideWindow:    true,
		CreationFlags: 0x08000000, // CREATE_NO_WINDOW，避免在前台 console 下闪窗
	}

	out, runErr := cmd.CombinedOutput()
	if ctx.Err() == context.DeadlineExceeded {
		return nil, fmt.Errorf("ping command timed out after %v", overall)
	}
	if runErr != nil && len(out) == 0 {
		return nil, fmt.Errorf("ping command failed: %w", runErr)
	}

	stats := &pingStats{PacketsSent: count}
	var totalRtt int64
	for _, line := range bytes.Split(out, []byte("\n")) {
		if !winPingReplyAnchor.Match(line) {
			continue
		}
		m := winPingTimeRe.FindSubmatch(line)
		if m == nil {
			continue
		}
		rtt, err := strconv.Atoi(string(m[1]))
		if err != nil {
			continue
		}
		stats.PacketsRecv++
		totalRtt += int64(rtt)
	}

	if stats.PacketsRecv > 0 {
		stats.AvgRttMs = totalRtt / int64(stats.PacketsRecv)
	}
	stats.PacketLoss = float64(stats.PacketsSent-stats.PacketsRecv) * 100 / float64(stats.PacketsSent)
	return stats, nil
}
