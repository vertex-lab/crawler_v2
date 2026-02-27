package watchdog

import (
	"testing"
	"time"
)

func TestWatchdog_ArmDisarm(t *testing.T) {
	timedOut := false
	wd := New(1*time.Second, func() {
		timedOut = true
	})

	start := time.Now()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	disarmAt := time.After(900 * time.Millisecond)

	done := false
	for !done {
		select {
		case tick := <-ticker.C:
			elapsed := tick.Sub(start).Round(time.Millisecond)
			t.Logf("[%s] calling Arm", elapsed)
			wd.Arm()

		case <-disarmAt:
			elapsed := time.Since(start).Round(time.Millisecond)
			t.Logf("[%s] calling Disarm", elapsed)
			wd.Disarm()
			done = true
		}
	}

	// Wait a bit past the original 1s timeout to confirm it never fired.
	time.Sleep(200 * time.Millisecond)

	if timedOut {
		t.Fatal("watchdog fired after Disarm, expected no timeout")
	}
	t.Log("OK: watchdog did not fire after Disarm")
}
