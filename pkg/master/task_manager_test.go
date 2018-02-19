package master

import (
	"testing"
	"time"

	"github.com/mlmhl/mapreduce/pkg/types"
)

func TestTaskState(t *testing.T) {
	tasks := []types.Task{{Type: types.Reduce, Index: 0}, {Type: types.Reduce, Index: 1}}
	ts := newTaskState(tasks)

	for _, target := range tasks {
		task, finished := ts.next()
		validateFinished(finished, false, t)
		validateTask(task, target, t)
	}

	c := make(chan types.Task)
	go func() {
		task, finished := ts.next()
		validateFinished(finished, false, t)
		c <- task
	}()

	select {
	case task := <-c:
		t.Fatalf("Reveive unexpected task: %+v", task)
	case <-time.After(time.Second * 2):
	}

	ts.report(tasks[0], types.Result{Code: types.ReduceErr})

	select {
	case task := <-c:
		validateTask(task, tasks[0], t)
	case <-time.After(time.Second * 2):
		t.Fatal("Should receive a task")
	}

	ts.report(tasks[0], types.SucceededResult)
	ts.report(tasks[1], types.SucceededResult)

	_, finished := ts.next()
	validateFinished(finished, true, t)
}

func validateFinished(finished, targetFinished bool, t *testing.T) {
	if finished != targetFinished {
		t.Fatalf("Wrong finished: expected %t, got %t", targetFinished, finished)
	}
}

func validateTask(task, targetTask types.Task, t *testing.T) {
	if task != targetTask {
		t.Fatalf("Wrong task: expected %+v, got %+v", targetTask, task)
	}
}
