package worker

import (
	"testing"
	"time"

	"github.com/mlmhl/mapreduce/pkg/types"
	"github.com/mlmhl/mapreduce/pkg/util"
)

func TestWorker(t *testing.T) {
	job := types.Job{Name: "test"}
	factory := newFakExecutorFactory()
	worker := newWorker("test", 1, job, factory)

	result := &types.Result{}

	stopChan := make(chan struct{})
	execute := func(typ types.TaskType) {
		defer func() { stopChan <- struct{}{} }()
		if err := worker.execute(&types.Task{Type: typ}, result); err != nil {
			t.Fatalf("Unexpected execute error: %v", err)
		}
	}

	validate := func(target types.WorkerStatus) {
		status := worker.Status()
		if status != target {
			t.Fatalf("Wrong worker status: expected %+v, got %+v", target, status)
		}
	}

	go execute(types.Map)
	select {
	case <-stopChan:
		t.Fatal("Executor shouldn't complete")
	case <-time.After(time.Second * 2):
	}
	validate(types.WorkerStatus{Name: "test", Running: 1})

	// Execute failed.
	factory.result <- types.Result{Code: types.MapErr}
	select {
	case <-stopChan:
	case <-time.After(time.Second * 2):
		t.Fatal("Executor should complete")
	}
	validate(types.WorkerStatus{Name: "test", Failed: 1})

	go execute(types.Reduce)
	select {
	case <-stopChan:
		t.Fatal("Executor shouldn't complete")
	case <-time.After(time.Second * 2):
	}
	validate(types.WorkerStatus{Name: "test", Running: 1, Failed: 1})

	shutdownChan := make(chan struct{})
	go func() {
		defer close(shutdownChan)
		worker.shutdown()
	}()

	select {
	case <-shutdownChan:
		t.Fatal("Shouldn't shutdown")
	case <-time.After(time.Second * 2):
	}

	// Execute succeeded.
	factory.result <- types.SucceededResult
	select {
	case <-stopChan:
	case <-time.After(time.Second * 2):
		t.Fatal("Executor should complete")
	}
	validate(types.WorkerStatus{Name: "test", Failed: 1, Finished: 1})

	select {
	case <-shutdownChan:
	case <-time.After(time.Second * 2):
		t.Fatal("Should shutdown")
	}
}

func newFakExecutorFactory() fakeExecutorFactory {
	return fakeExecutorFactory{result: make(chan types.Result)}
}

type fakeExecutorFactory struct {
	result chan types.Result
}

func (f fakeExecutorFactory) Executor(task types.Task, job types.Job) util.Executor {
	return &fakeExecutor{result: f.result}
}

type fakeExecutor struct {
	result chan types.Result
}

func (e *fakeExecutor) Run() types.Result {
	return <-e.result
}
