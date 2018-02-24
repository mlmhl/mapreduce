package master

import (
	"fmt"

	"github.com/mlmhl/mapreduce/pkg/types"

	"github.com/golang/glog"
	"github.com/mlmhl/mapreduce/pkg/util"
)

func New(job types.Job, mode types.Mode, inputFiles []string) (*Master, error) {
	if err := validate(job, mode, inputFiles); err != nil {
		return nil, err
	}
	var scheduler scheduler
	var taskManager taskManager
	switch mode {
	case types.Sequential:
		taskManager = newSequentialTaskManager(job, inputFiles)
		scheduler = newSequentialScheduler(job, taskManager, util.NewExecutorFactory())
	}
	return &Master{scheduler: scheduler, taskManager: taskManager, finishedChan: make(chan struct{})}, nil
}

func validate(job types.Job, mode types.Mode, inputFiles []string) error {
	if job.MapNum != len(inputFiles) {
		return fmt.Errorf("mismatch map number(%d) and input file number(%d)", job.MapNum, len(inputFiles))
	}
	if !types.ValidaMode(mode) {
		return fmt.Errorf("unsupported mode: %s", mode)
	}
	return nil
}

type Master struct {
	job         types.Job
	scheduler   scheduler
	taskManager taskManager

	finalStatus types.JobStatus

	finishedChan chan struct{}
}

func (m *Master) Run() types.JobStatus {
	defer close(m.finishedChan)
	defer glog.Infof("Job %s completed", m.job.Name)
	glog.Infof("Job %s started", m.job.Name)
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
	go func() {
		m.Run()
	}()
}

func (m *Master) Status() types.JobStatus {
	select {
	case <-m.finishedChan:
		return m.finalStatus
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
