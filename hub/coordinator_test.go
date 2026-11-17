package hub

import (
	"sync"
	"testing"
	"time"
)

// hammerCoordinator runs check in a tight loop alongside Register/UnregisterWorker,
// whose write locks keep queueing behind its read locks, and fails if the two
// loops cannot be stopped. A reader that takes coordinatorMutex.RLock twice
// deadlocks as soon as a writer queues between the two: sync.RWMutex makes the
// second RLock wait for that writer, which waits for the first RLock.
func hammerCoordinator(t *testing.T, check func(pwc *ParallelWorkerCoordinator)) {
	t.Helper()
	pwc := NewParallelWorkerCoordinator()
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				check(pwc)
			}
		}
	}()
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				pwc.RegisterWorker(1)
				pwc.UnregisterWorker(1)
			}
		}
	}()
	time.Sleep(200 * time.Millisecond)
	close(stop)

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("coordinatorMutex deadlocked: a reader re-acquired the read lock behind a pending writer")
	}
}

func TestCheckForIdleWorkersDoesNotDeadlock(t *testing.T) {
	hammerCoordinator(t, func(pwc *ParallelWorkerCoordinator) { pwc.CheckForIdleWorkers() })
}

func TestCoordinatorTimeoutSnapshotDoesNotDeadlock(t *testing.T) {
	hammerCoordinator(t, func(pwc *ParallelWorkerCoordinator) { pwc.timeoutSnapshot() })
}

// The worker timeout is off unless STEAMPIPE_FDW_WORKER_TIMEOUT sets it: a worker running a
// long Parallel Append branch must not be cut, however long it has been registered.
func TestWorkerTimeoutIsOffByDefault(t *testing.T) {
	t.Setenv("STEAMPIPE_FDW_WORKER_TIMEOUT", "")
	pwc := NewParallelWorkerCoordinator()
	pwc.RegisterWorker(1)
	pwc.executionStart = time.Now().Add(-time.Hour).Unix()

	if pwc.Timeout() != 0 {
		t.Errorf("default worker timeout is %d seconds, expected it off", pwc.Timeout())
	}
	if pwc.ShouldWorkerTimeout() {
		t.Error("a worker registered an hour ago timed out with no STEAMPIPE_FDW_WORKER_TIMEOUT set")
	}
	if idle := pwc.CheckForIdleWorkers(); len(idle) != 0 {
		t.Errorf("CheckForIdleWorkers reported %v with the timeout off", idle)
	}
}

func TestWorkerTimeoutFromEnv(t *testing.T) {
	t.Setenv("STEAMPIPE_FDW_WORKER_TIMEOUT", "30")
	pwc := NewParallelWorkerCoordinator()
	pwc.RegisterWorker(1)

	if pwc.Timeout() != 30 {
		t.Fatalf("worker timeout is %d seconds, expected the 30 STEAMPIPE_FDW_WORKER_TIMEOUT sets", pwc.Timeout())
	}
	if pwc.ShouldWorkerTimeout() {
		t.Error("a worker that just registered timed out")
	}
	pwc.executionStart = time.Now().Add(-31 * time.Second).Unix()
	if !pwc.ShouldWorkerTimeout() {
		t.Error("a worker registered 31 seconds ago did not time out with a 30 second timeout")
	}
}
