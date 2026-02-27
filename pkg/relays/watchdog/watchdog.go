package watchdog

import (
	"sync"
	"time"
)

// T is a thread-safe watchdog timer with a specified duration.
// When the timer fires, it calls the specified onTimeout function in a separate goroutine.
// Call Arm and Disarm to start and stop the timer respectively.
type T struct {
	mu        sync.Mutex
	timer     *time.Timer
	duration  time.Duration
	onTimeout func()
}

func New(duration time.Duration, onTimeout func()) *T {
	return &T{
		duration:  duration,
		onTimeout: onTimeout,
	}
}

// Arm starts the watchdog timer. If the timer is already running from a
// previous Arm call, it is stopped and restarted.
func (w *T) Arm() {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.timer != nil {
		w.timer.Stop()
	}
	w.timer = time.AfterFunc(w.duration, w.onTimeout)
}

// Disarm stops the watchdog timer, cancelling the pending timeout.
// Safe to call even if Arm has never been called.
func (w *T) Disarm() {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.timer != nil {
		w.timer.Stop()
	}
}
