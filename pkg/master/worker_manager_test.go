package master

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestWorkerQueue(t *testing.T) {
	const n = 5
	wq := newTestWorkerQueue(n)

	// Test for normal allocate and release.
	for i := 0; i < n; i++ {
		name := testWorkerName(i)
		wq.release(name)
		w, available := wq.allocate()
		validateAllocatedWorker(t, w.name, name, available, true)
	}

	// Test for normal allocate and update.
	for i := 0; i < n; i++ {
		name := testWorkerName(i)
		wq.update(name, 1)
		w, available := wq.allocate()
		validateAllocatedWorker(t, w.name, name, available, true)
	}

	// Test for remove.
	for i := 0; i < n; i++ {
		wq.update(testWorkerName(i), i)
	}
	wq.remove(testWorkerName(n - 1))
	w, available := wq.allocate()
	validateAllocatedWorker(t, w.name, testWorkerName(n-2), available, true)
}

func TestWorkerManager(t *testing.T) {
	const n = 5
	wm := newTestWorkerManager(n)
	workerChan := make(chan worker)

	// Test for normal Allocate and Release.
	go func() {
		w, stopped := wm.Allocate()
		if stopped {
			t.Fatalf("WorkerManager stopped unexpected")
		}
		workerChan <- w
	}()
	select {
	case w := <-workerChan:
		t.Fatalf("Receive unexpected worker: %s", w.String())
	case <-time.After(time.Second):
	}
	wm.Release(testWorkerName(0))
	select {
	case w := <-workerChan:
		validateAllocatedWorker(t, w.name, testWorkerName(0), true, true)
	case <-time.After(time.Second):
		t.Fatal("Should receive a worker")
	}

	// Test for Taint.
	for i := 0; i < n; i++ {
		wm.queue.update(testWorkerName(i), i)
	}
	wm.Taint(testWorkerName(n - 1))
	w, stopped := wm.Allocate()
	if stopped {
		t.Fatalf("WorkerManager stopped unexpected")
	}
	validateAllocatedWorker(t, w.name, testWorkerName(n-2), true, true)

	// Test for outdated worker cleanup.
	wm.workerTimeout = time.Minute
	for i := 0; i < n-2; i++ {
		wm.queue.heartBeat(testWorkerName(i), time.Now())
	}
	wm.queue.heartBeat(testWorkerName(n-2), time.Now().Add(-(time.Minute + time.Second)))
	wm.cleanupUnhealthyWorkers()
	w, stopped = wm.Allocate()
	if stopped {
		t.Fatalf("WorkerManager stopped unexpected")
	}
	validateAllocatedWorker(t, w.name, testWorkerName(n-3), true, true)
}

func newTestWorkerQueue(size int) workerQueue {
	wq := newWorkerQueue()
	for i := 0; i < size; i++ {
		wq.add(worker{name: testWorkerName(i)}, 0)
	}
	return wq
}

func newTestWorkerManager(size int) defaultWorkerManager {
	lock := &sync.Mutex{}
	return defaultWorkerManager{
		lock: lock,
		cond: sync.NewCond(lock),

		queue: newTestWorkerQueue(size),
	}
}

func testWorkerName(index int) string {
	return fmt.Sprintf("worker-%d", index)
}

func validateAllocatedWorker(t *testing.T, name, targetName string, available, targetAvailable bool) {
	if available != targetAvailable {
		if targetAvailable {
			t.Fatal("Should have available worker")
		} else {
			t.Fatal("Shouldn't have available worker")
		}
	}
	if name != targetName {
		t.Fatalf("Allocate wrong worker, expected %s, got %s", targetName, name)
	}
}
