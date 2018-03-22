package master

import (
	"encoding/json"
	"fmt"
	"sort"

	"github.com/golang/glog"
	"github.com/mlmhl/mapreduce/pkg/types"
	"github.com/mlmhl/mapreduce/pkg/util"
)

type scheduler interface {
	Run() (string, error)
}

func mergeReduceOutputFiles(job types.Job) (string, error) {
	files := make([]string, job.ReduceNum)
	defer func() {
		for _, fileName := range files {
			job.Storage.Remove(fileName)
		}
	}()

	var kvs []types.KeyValue

	for i := 0; i < job.ReduceNum; i++ {
		fileName := util.GenerateReduceOutputFileName(job.Name, i)
		file, err := job.Storage.Open(fileName)
		if err != nil {
			return "", err
		}
		files[i] = fileName

		kvList, err := util.ReadFile(file)
		if err != nil {
			return "", err
		}
		kvs = append(kvs, kvList...)

		if err = file.Close(); err != nil {
			return "", err
		}
	}

	sort.Slice(kvs, func(i, j int) bool {
		return kvs[i].Key < kvs[j].Key
	})

	outputFileName := generateFinalOutputFileName(job.Name)
	file, err := job.Storage.Create(outputFileName)
	if err != nil {
		return outputFileName, err
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	for _, kv := range kvs {
		if err = encoder.Encode(&kv); err != nil {
			return outputFileName, err
		}
	}

	return outputFileName, nil
}

func generateFinalOutputFileName(jobName string) string {
	return fmt.Sprintf("%s-%s", types.FinalOutputFIlePrefix, jobName)
}

func newSequentialScheduler(
	job types.Job,
	manager taskManager,
	executorFactory util.ExecutorFactory) scheduler {
	return &sequentialScheduler{
		job:             job,
		manager:         manager,
		executorFactory: executorFactory,
	}
}

type sequentialScheduler struct {
	job             types.Job
	manager         taskManager
	executorFactory util.ExecutorFactory
}

func (s *sequentialScheduler) Run() (string, error) {
	for {
		task, finished := s.manager.Next()
		if finished {
			break
		}
		s.manager.Report(task, s.runOne(task))
	}

	return mergeReduceOutputFiles(s.job)
}

func (s *sequentialScheduler) runOne(task types.Task) types.Result {
	var result types.Result

	switch task.Type {
	case types.Map, types.Reduce:
		glog.Infof("Start task: %s", task.Key())
		result = s.execute(task)
	default:
		result = types.Result{Code: types.UnknownTask, Message: fmt.Sprintf("Unknown task type: %s", task.Type)}
	}

	return result
}

func (s *sequentialScheduler) execute(task types.Task) types.Result {
	return s.executorFactory.Executor(task, s.job).Run()
}

func newParallelScheduler(job types.Job, taskManager taskManager, workerManager workerManager) scheduler {
	return &parallelScheduler{
		job:           job,
		taskManager:   taskManager,
		workerManager: workerManager,
	}
}

type parallelScheduler struct {
	job           types.Job
	taskManager   taskManager
	workerManager workerManager
}

func (s *parallelScheduler) Run() (string, error) {
	for {
		task, finished := s.taskManager.Next()
		if finished {
			break
		}
		go s.taskManager.Report(task, s.runOne(task))
	}
	return mergeReduceOutputFiles(s.job)
}

func (s *parallelScheduler) runOne(task types.Task) types.Result {
	worker, stopped := s.workerManager.Allocate()
	if stopped {
		return types.Result{Code: types.ExceptionErr, Message: "Master stopped unexpected"}
	}
	defer s.workerManager.Release(worker.name)

	glog.V(3).Infof("Assign task %s to worker %s", task.Key(), worker.String())

	result := types.Result{}
	err := worker.Execute(&task, &result)
	if err != nil {
		result.Code = types.RpcCallErr
		result.Message = fmt.Sprintf("Rpc call(Execute) failed: %v", err)
	}
	return result
}
