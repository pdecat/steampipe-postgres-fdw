package hub

import (
	"bytes"
	"log"
	"strings"
	"testing"
	"time"
)

// runCancelStalls feeds reportCancelStalls one tick per second for the given
// pending states and returns how long the request had been pending at each dump.
func runCancelStalls(states []bool) []time.Duration {
	tick := make(chan time.Time)
	state := make(chan bool)
	done := make(chan struct{})
	var dumps []time.Duration
	go func() {
		reportCancelStalls(tick, func() bool { return <-state }, []time.Duration{30 * time.Second, 5 * time.Minute}, func(d time.Duration) {
			dumps = append(dumps, d)
		})
		close(done)
	}()
	start := time.Unix(0, 0)
	for i, s := range states {
		tick <- start.Add(time.Duration(i) * time.Second)
		state <- s
	}
	close(tick)
	<-done
	return dumps
}

func repeat(state bool, n int) []bool {
	states := make([]bool, n)
	for i := range states {
		states[i] = state
	}
	return states
}

func TestReportCancelStallsNeverPending(t *testing.T) {
	if dumps := runCancelStalls(repeat(false, 600)); len(dumps) != 0 {
		t.Fatalf("expected no dump, got %v", dumps)
	}
}

func TestReportCancelStallsServicedQuickly(t *testing.T) {
	var states []bool
	for i := 0; i < 20; i++ {
		states = append(states, repeat(true, 10)...)
		states = append(states, false)
	}
	if dumps := runCancelStalls(states); len(dumps) != 0 {
		t.Fatalf("expected no dump for requests serviced within 10s, got %v", dumps)
	}
}

func TestReportCancelStallsDumpsOncePerThreshold(t *testing.T) {
	dumps := runCancelStalls(repeat(true, 900))
	want := []time.Duration{30 * time.Second, 5 * time.Minute}
	if len(dumps) != len(want) {
		t.Fatalf("expected dumps at %v, got %v", want, dumps)
	}
	for i := range want {
		if dumps[i] != want[i] {
			t.Fatalf("expected dumps at %v, got %v", want, dumps)
		}
	}
}

func TestReportCancelStallsRestartsAfterClear(t *testing.T) {
	states := append(repeat(true, 40), false)
	states = append(states, repeat(true, 40)...)
	dumps := runCancelStalls(states)
	if len(dumps) != 2 || dumps[0] != 30*time.Second || dumps[1] != 30*time.Second {
		t.Fatalf("expected one 30s dump per pending episode, got %v", dumps)
	}
}

func TestDumpGoroutinesLogsEveryLineWithPid(t *testing.T) {
	var out bytes.Buffer
	defer log.SetOutput(log.Writer())
	log.SetOutput(&out)

	dumpGoroutines(31 * time.Second)

	lines := strings.Split(strings.TrimRight(out.String(), "\n"), "\n")
	if !strings.Contains(lines[0], "[WARN] Worker PID ") || !strings.Contains(lines[0], "pending for 31s") {
		t.Fatalf("unexpected header line %q", lines[0])
	}
	for _, line := range lines[1:] {
		if !strings.Contains(line, "[WARN] Worker PID ") || !strings.Contains(line, ": stack: ") {
			t.Fatalf("stack line without the pid prefix: %q", line)
		}
	}
	if !strings.Contains(out.String(), "TestDumpGoroutinesLogsEveryLineWithPid") {
		t.Fatal("dump does not include the calling goroutine's stack")
	}
}
