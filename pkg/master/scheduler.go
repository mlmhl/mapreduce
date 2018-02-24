package master

import (
	"encoding/json"
	"fmt"
	"sort"

	"github.com/mlmhl/mapreduce/pkg/types"
	"github.com/mlmhl/mapreduce/pkg/util"
)

type scheduler interface {
	Run() (string, error)
}

type schedulerImpl interface {
	runOne(task types.Task) types.Result
}

func newGenericScheduler(job types.Job, manager taskManager) genericScheduler {
	return genericScheduler{
		job:     job,
		manager: manager,
	}
}

type genericScheduler struct {
	impl schedulerImpl

	job        types.Job
	inputFiles []string
	manager    taskManager
}

func (s *genericScheduler) Run() (string, error) {
	for {
		task, finished := s.manager.Next()
		if finished {
			break
		}
		s.manager.Report(task, s.impl.runOne(task))
	}

	return s.mergeReduceOutputFiles()
}

func (s *genericScheduler) mergeReduceOutputFiles() (string, error) {
	files := make([]string, s.job.ReduceNum)
	defer func() {
		for _, fileName := range files {
			s.job.Storage.Remove(fileName)
		}
	}()

	var kvs []types.KeyValue

	for i := 0; i < s.job.ReduceNum; i++ {
		fileName := util.GenerateReduceOutputFileName(s.job.Name, i)
		file, err := s.job.Storage.Open(fileName)
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

	outputFileName := generateFinalOutputFileName(s.job.Name)
	file, err := s.job.Storage.Create(outputFileName)
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
	s := &sequentialScheduler{
		genericScheduler: newGenericScheduler(job, manager),
		executorFactory:  executorFactory,
	}
	s.impl = s
	return s
}

type sequentialScheduler struct {
	genericScheduler
	executorFactory util.ExecutorFactory
}

func (s *sequentialScheduler) runOne(task types.Task) types.Result {
	var result types.Result

	switch task.Type {
	case types.Map, types.Reduce:
		result = s.execute(task)
	default:
		result = types.Result{Code: types.UnknownTask, Message: fmt.Sprintf("Unknown task type: %s", task.Type)}
	}

	return result
}

func (s *sequentialScheduler) execute(task types.Task) types.Result {
	return s.executorFactory.Executor(task, s.job).Run()
}
