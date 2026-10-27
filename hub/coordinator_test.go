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
