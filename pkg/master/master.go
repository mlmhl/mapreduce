package master

import (
	"fmt"

	"github.com/mlmhl/mapreduce/pkg/storage"
	"github.com/mlmhl/mapreduce/pkg/types"

	"github.com/golang/glog"
)

func New(job types.Job, mode types.Mode, storage storage.Storage) (*Master, error) {
	if err := validate(job, mode); err != nil {
		return nil, err
	}
	var scheduler scheduler
	var taskManager taskManager
	switch mode {
	case types.Sequential:
		taskManager = newSequentialTaskManager(job)
		scheduler = newSequentialScheduler(job.MetaData, taskManager, storage)
	}
	return &Master{scheduler: scheduler, taskManager: taskManager, finishedChan: make(chan struct{})}, nil
}

func validate(job types.Job, mode types.Mode) error {
	if job.MapNum != len(job.InputFiles) {
		return fmt.Errorf("mismatch map number(%d) and input file number(%d)", job.MapNum, len(job.InputFiles))
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
