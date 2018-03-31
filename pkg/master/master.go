package master

import (
	"fmt"
	"time"

	"github.com/mlmhl/mapreduce/pkg/rpc"
	"github.com/mlmhl/mapreduce/pkg/types"
	"github.com/mlmhl/mapreduce/pkg/util"

	"github.com/golang/glog"
)

func New(config types.MasterConfiguration) (*Master, error) {
	if err := validate(config); err != nil {
		return nil, err
	}

	m := &Master{
		job:          config.Job,
		taskManager:  newTaskManager(config.Job, config.InputFiles),
		finishedChan: make(chan struct{}),
	}

	var scheduler scheduler
	var workerManager workerManager
	switch config.Mode {
	case types.Sequential:
		scheduler = newSequentialScheduler(config.Job, m.taskManager, util.NewExecutorFactory())
	case types.Parallel:
		rpcServer, err := newServer(m, config.Address)
		if err != nil {
			return nil, err
		}
		m.rpcServer = rpcServer
		workerManager = newWorkerManager(config.WorkerTimeout, config.CheckInterval)
		scheduler = newParallelScheduler(config.Job, m.taskManager, workerManager)
	default:
		return nil, fmt.Errorf("unknown mode: %s", config.Mode)
	}

	m.scheduler = scheduler
	m.workerManager = workerManager

	return m, nil
}

func validate(config types.MasterConfiguration) error {
	if config.Job.MapNum != len(config.InputFiles) {
		return fmt.Errorf("mismatch map number(%d) and input file number(%d)", config.Job.MapNum, len(config.InputFiles))
	}
	if !types.ValidaMode(config.Mode) {
		return fmt.Errorf("unsupported mode: %s", config.Mode)
	}
	return nil
}

type Master struct {
	job         types.Job
	scheduler   scheduler
	taskManager taskManager

	finalStatus types.JobStatus

	finishedChan chan struct{}

	// Special for parallel mode.
	rpcServer     *Server
	workerManager workerManager
}

func (m *Master) Run() types.JobStatus {
	defer glog.Infof("Job %s completed", m.job.Name)
	glog.Infof("Job %s started", m.job.Name)

	defer close(m.finishedChan)

	if m.rpcServer != nil {
		m.rpcServer.run()
		defer m.rpcServer.stop()
	}

	if m.workerManager != nil {
		m.workerManager.Run()
		defer m.workerManager.Stop()
	}

	output, err := m.scheduler.Run()
	status := m.taskManager.Status()
	if err == nil {
		status.Output = output
		status.Message = "Succeeded"
	} else {
		status.Message = fmt.Sprintf("Failed: %s", err.Error())
	}
	m.finalStatus = status
	return status
}

func (m *Master) Go() {
	go m.Run()
}

func (m *Master) Status() types.JobStatus {
	select {
	case <-m.finishedChan:
		return m.finalStatus
	default:
	}
	status := m.taskManager.Status()
	status.Message = "Running"
	return status
}

func (m *Master) Finished() bool {
	select {
	case <-m.finishedChan:
		return true
	}
	return false
}

func (m *Master) Wait() {
	<-m.finishedChan
}

func (m *Master) addWorker(name string, balance int, address rpc.Address) error {
	w, err := newWorker(name, address)
	if err != nil {
		return err
	}
	m.workerManager.Add(w, balance)
	return nil
}

func newWorker(name string, address rpc.Address) (worker, error) {
	client, err := rpc.NewClient(address)
	if err != nil {
		return worker{}, err
	}
	return worker{
		name:       name,
		lastActive: time.Now(),
		client:     client,
	}, nil
}

func (m *Master) workerHeartBeat(name string) {
	m.workerManager.HeartBeat(name)
}
