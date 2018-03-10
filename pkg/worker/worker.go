package worker

import (
	"fmt"
	"sync"
	"time"

	"github.com/mlmhl/mapreduce/pkg/rpc"
	"github.com/mlmhl/mapreduce/pkg/types"
	"github.com/mlmhl/mapreduce/pkg/util"

	"github.com/golang/glog"
)

// TODO: Add a Debug mode so that we can simulate network partition, software bug and hardware failure.

const defaultHealthyReportInterval = time.Second * 30

func New(config types.WorkerConfiguration) (*Worker, error) {
	w := newWorker(config.Name, config.Limit, config.Job, util.NewExecutorFactory())

	client, err := rpc.NewClient(config.MasterAddress)
	if err != nil {
		return nil, fmt.Errorf("create rpc client failed: %v", err)
	}
	w.client = client

	rpcServer, err := newRpcServer(w, config.Address)
	if err != nil {
		return nil, err
	}
	w.rpcServer = rpcServer

	// We should register to master only after the rpc server started listening.
	if err := w.register(); err != nil {
		return nil, err
	}

	go w.healthyReportLoop(config.HealthyReportInterval)

	return w, nil
}

func newWorker(name string, limit int, job types.Job, executorFactory util.ExecutorFactory) *Worker {
	lock := &sync.Mutex{}
	cond := sync.NewCond(lock)
	return &Worker{
		name: name,
		job:  job,

		cond:     cond,
		lock:     lock,
		limit:    limit,
		stopChan: make(chan struct{}),

		executorFactory: executorFactory,
	}
}

type Worker struct {
	name string
	job  types.Job

	cond     *sync.Cond
	lock     *sync.Mutex
	limit    int
	running  int
	failed   int
	finished int
	stopChan chan struct{}

	client          *rpc.Client
	rpcServer       *Server
	executorFactory util.ExecutorFactory
}

func (w *Worker) Run() {
	w.rpcServer.run()
	<-w.stopChan
	w.waitForTasksFinished()
}

func (w *Worker) Go() {
	go w.Run()
}

func (w *Worker) Finished() bool {
	select {
	case <-w.stopChan:
	default:
		return false
	}

	w.lock.Lock()
	defer w.lock.Unlock()
	return w.running == 0
}

func (w *Worker) Status() types.WorkerStatus {
	w.lock.Lock()
	defer w.lock.Unlock()
	return types.WorkerStatus{
		Name:     w.name,
		Running:  w.running,
		Failed:   w.failed,
		Finished: w.finished,
	}
}

func (w *Worker) register() error {
	result := &types.RegisterResult{}
	arg := types.RegisterArg{Name: w.name, Address: w.rpcServer.address(), Balance: w.limit}
	err := w.client.Call(w.masterMethodName("Register"), arg, result)
	if err != nil {
		return fmt.Errorf("rpc call failed: %v", err)
	}
	if result.Code != 0 {
		return fmt.Errorf("register failed: %s", result.Message)
	}

	return nil
}

func (w *Worker) healthyReportLoop(interval time.Duration) {
	if interval == 0 {
		interval = defaultHealthyReportInterval
	}
	timer := time.NewTimer(interval)
	for {
		err := w.client.Call(w.masterMethodName("Ping"), types.PingArg{Name: w.name}, &types.Result{})
		if err != nil {
			glog.Warningf("Ping master failed: %v", err)
		}
		<-timer.C
		timer.Reset(interval)
	}
}

func (w *Worker) masterMethodName(method string) string {
	return fmt.Sprintf("%s.%s", w.job.Name, method)
}

func (w *Worker) execute(task *types.Task, result *types.Result) error {
	w.lock.Lock()
	defer w.lock.Unlock()

	if w.running >= w.limit {
		msg := fmt.Sprintf("Worker %s: Current running task number(%d) reach the limit(%d)",
			w.name, w.running, w.limit)
		*result = types.Result{Code: types.ParallelLimitErr, Message: msg}
		return nil
	}

	w.running++
	defer func() {
		w.running--
		w.cond.Broadcast()
	}()

	func() {
		// Run the task out of critical region.
		w.lock.Unlock()
		defer w.lock.Lock()
		switch task.Type {
		case types.Map, types.Reduce:
			*result = w.executorFactory.Executor(*task, w.job).Run()
		default:
			*result = types.Result{
				Code:    types.UnknownTask,
				Message: fmt.Sprintf("Unknown task type: %s", task.Type),
			}
		}
	}()

	if result.Code == 0 {
		w.finished++
	} else {
		w.failed++
	}

	return nil
}

func (w *Worker) shutdown() {
	if w.client != nil {
		w.client.Close()
	}
	close(w.stopChan)
	w.waitForTasksFinished()
	// We won't close rpcServer here, as after this method returned, rpcServer will close itself.
}

func (w *Worker) waitForTasksFinished() {
	w.lock.Lock()
	defer w.lock.Unlock()
	for w.running > 0 {
		w.cond.Wait()
	}
}
