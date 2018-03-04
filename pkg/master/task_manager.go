package master

import (
	"sync"

	"github.com/mlmhl/mapreduce/pkg/types"

	"github.com/golang/glog"
)

type taskManager interface {
	Next() (types.Task, bool)
	Status() types.JobStatus
	Report(task types.Task, result types.Result)
}

func newTaskManager(job types.Job, inputFiles []string) *genericTaskManager {
	mapTasks := make([]types.Task, job.MapNum)
	for i := 0; i < job.MapNum; i++ {
		mapTasks[i] = types.Task{Type: types.Map, Index: i, FileName: inputFiles[i]}
	}
	reduceTasks := make([]types.Task, job.ReduceNum)
	for i := 0; i < job.ReduceNum; i++ {
		reduceTasks[i] = types.Task{Type: types.Reduce, Index: i}
	}

	return &genericTaskManager{
		jobName:     job.Name,
		mapTasks:    newTaskState(mapTasks),
		reduceTasks: newTaskState(reduceTasks),
	}
}

type genericTaskManager struct {
	jobName     string
	mapTasks    tasksState
	reduceTasks tasksState
}

func (tm *genericTaskManager) Next() (types.Task, bool) {
	task, finished := tm.mapTasks.next()
	if !finished {
		return task, false
	}
	return tm.reduceTasks.next()
}

func (tm *genericTaskManager) Status() types.JobStatus {
	return types.JobStatus{
		MapStatus:    tm.mapTasks.status(),
		ReduceStatus: tm.reduceTasks.status(),
	}
}

func (tm *genericTaskManager) Report(task types.Task, result types.Result) {
	switch task.Type {
	case types.Map:
		tm.mapTasks.report(task, result)
	case types.Reduce:
		tm.reduceTasks.report(task, result)
	}
}

type taskSet map[string]types.Task

func newTaskState(tasks []types.Task) tasksState {
	lock := &sync.Mutex{}
	cond := sync.NewCond(lock)

	set := taskSet{}
	for _, task := range tasks {
		set[task.Key()] = task
	}

	return tasksState{
		cond:  cond,
		lock:  lock,
		total: len(tasks),
		tasks: map[types.TaskState]taskSet{types.Pending: set, types.Running: {}, types.Finished: {}},
	}
}

type tasksState struct {
	cond *sync.Cond
	lock *sync.Mutex

	total    int
	running  int
	finished int
	tasks    map[types.TaskState]taskSet
}

func (ts *tasksState) next() (types.Task, bool) {
	ts.lock.Lock()
	defer ts.lock.Unlock()

	for ts.finished < ts.total && len(ts.tasks[types.Pending]) == 0 {
		ts.cond.Wait()
	}

	if ts.finished >= ts.total {
		return types.Task{}, true
	}

	var task types.Task
	for _, task = range ts.tasks[types.Pending] {
		break
	}

	ts.running++
	delete(ts.tasks[types.Pending], task.Key())
	ts.tasks[types.Running][task.Key()] = task

	glog.Infof("Start task: %s", task.Key())

	return task, false
}

func (ts *tasksState) status() types.TasksStatus {
	ts.lock.Lock()
	defer ts.lock.Unlock()
	return types.TasksStatus{
		Total:    ts.total,
		Running:  ts.running,
		Finished: ts.finished,
	}
}

func (ts *tasksState) report(task types.Task, result types.Result) {
	ts.lock.Lock()
	defer ts.lock.Unlock()

	ts.running--
	delete(ts.tasks[types.Running], task.Key())

	if result.Code == 0 {
		// Succeeded
		ts.finished++
		ts.tasks[types.Finished][task.Key()] = task
		glog.Infof("Task succeeded: %s", task.Key())
	} else {
		// Failed, push back to the pending queue and try to execute later.
		ts.tasks[types.Pending][task.Key()] = task
		glog.Infof("Task failed: %s, %s", task.Key(), result.Message)
	}

	ts.cond.Broadcast()
}
