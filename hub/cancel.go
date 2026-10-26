package hub

import (
	"log"
	"os"
	"runtime"
	"strings"
	"time"
)

// queryCancelPollInterval is how often the per-scan watcher goroutine asks
// Postgres whether the current backend has a pending cancellation. 250ms gives
// sub-second responsiveness to statement_timeout / pg_cancel_backend without
// noticeable cgo overhead (a single inline read of two sig_atomic_t globals).
const queryCancelPollInterval = 250 * time.Millisecond

// cancelStallPollInterval is how often the stall reporter samples the pending
// flags. It only has to resolve the dump thresholds below, not react quickly.
const cancelStallPollInterval = time.Second

// cancelStallDumpThresholds are how long a cancel or terminate request may stay
// pending before the stall reporter dumps every goroutine's stack. A backend in
// a scan services one within a poll or two of the per-scan watcher, which
// cancels the plugin stream so that IterateForeignScan returns to Postgres.
// A request still pending after 30s means Go code is holding the backend
// somewhere the watcher does not reach. The second dump tells a goroutine
// blocked in place from one still making progress.
var cancelStallDumpThresholds = []time.Duration{30 * time.Second, 5 * time.Minute}

// cancelStallMaxDumpBytes caps the stack buffer so a runaway goroutine count
// cannot make the dump itself exhaust the backend's memory.
const cancelStallMaxDumpBytes = 64 << 20

// queryCancelChecker is set by the FDW (cgo) layer at init via
// SetQueryCancelChecker. It returns true when Postgres has set
// QueryCancelPending or ProcDiePending on the current backend. The hub uses
// this from a polling goroutine to bridge Postgres cancellation into the
// iterator's Go context, which is otherwise unreachable while the cgo
// IterateForeignScan call is blocked on a hung plugin gRPC stream (issue
// #671).
var queryCancelChecker func() bool

// SetQueryCancelChecker installs the function the hub uses to detect a
// pending Postgres query-cancel or backend-die request. It is intended to be
// called exactly once, from the FDW package's init(), with a cgo-backed
// implementation.
func SetQueryCancelChecker(fn func() bool) {
	queryCancelChecker = fn
}

// isQueryCancelPending reports whether Postgres has requested cancellation of
// the current backend. Returns false if no checker has been installed (e.g.
// in unit tests that exercise the hub in isolation).
func isQueryCancelPending() bool {
	if queryCancelChecker == nil {
		return false
	}
	return queryCancelChecker()
}

// queryCancelCheckerConfigured reports whether a cancellation checker has
// been registered. Callers can use this to skip starting the per-scan
// watcher goroutine entirely when the bridge isn't wired up.
func queryCancelCheckerConfigured() bool {
	return queryCancelChecker != nil
}

// StartCancelStallReporter starts a goroutine that logs every goroutine's
// stack at WARN when a Postgres cancel or terminate request goes unserviced,
// so that a backend ignoring statement_timeout, or a parallel worker ignoring
// its leader's terminate request (which leaves the leader waiting on
// BgworkerShutdown forever), shows where it is stuck. It is intended to be
// called once per backend, after SetQueryCancelChecker, and does nothing if no
// checker has been installed.
func StartCancelStallReporter() {
	if !queryCancelCheckerConfigured() {
		return
	}
	ticker := time.NewTicker(cancelStallPollInterval)
	go reportCancelStalls(ticker.C, isQueryCancelPending, cancelStallDumpThresholds, dumpGoroutines)
}

// reportCancelStalls calls dump once for each threshold a pending request
// outlives, timed from the first tick that saw it pending, and starts over
// once the request clears.
func reportCancelStalls(tick <-chan time.Time, pending func() bool, thresholds []time.Duration, dump func(pendingFor time.Duration)) {
	var pendingSince time.Time
	dumped := 0
	for now := range tick {
		if !pending() {
			pendingSince = time.Time{}
			dumped = 0
			continue
		}
		if pendingSince.IsZero() {
			pendingSince = now
			continue
		}
		if dumped < len(thresholds) && now.Sub(pendingSince) >= thresholds[dumped] {
			dump(now.Sub(pendingSince))
			dumped++
		}
	}
}

// dumpGoroutines logs every goroutine's stack, one log call per stack line so
// that each line still carries the backend pid once the server log is split
// into one entry per line.
func dumpGoroutines(pendingFor time.Duration) {
	pid := os.Getpid()
	buf := make([]byte, 1<<20)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) || len(buf) >= cancelStallMaxDumpBytes {
			buf = buf[:n]
			break
		}
		buf = make([]byte, 2*len(buf))
	}
	log.Printf("[WARN] Worker PID %d: Postgres cancel or terminate request pending for %s, dumping all goroutine stacks", pid, pendingFor.Round(time.Second))
	for _, line := range strings.Split(strings.TrimRight(string(buf), "\n"), "\n") {
		log.Printf("[WARN] Worker PID %d: stack: %s", pid, line)
	}
}
