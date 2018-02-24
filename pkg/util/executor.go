package util

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/mlmhl/mapreduce/pkg/storage"
	"github.com/mlmhl/mapreduce/pkg/types"
)

func NewExecutorFactory() ExecutorFactory {
	return factory{}
}

type ExecutorFactory interface {
	Executor(task types.Task, job types.Job) Executor
}

type factory struct{}

func (factory) Executor(task types.Task, job types.Job) Executor {
	if task.Type == types.Map {
		return newMapExecutor(task, job)
	}
	return newReduceExecutor(task, job)
}

type Executor interface {
	Run() types.Result
}

type executor struct {
	task types.Task
	job  types.Job
}

func newMapExecutor(task types.Task, job types.Job) Executor {
	return &mapExecutor{executor: executor{task: task, job: job}}
}

type mapExecutor struct {
	executor
	intermediateFiles map[string]storage.File
}

func (m *mapExecutor) Run() types.Result {
	file, err := m.job.Storage.Open(m.task.FileName)
	if err != nil {
		return types.Result{Code: types.FileIOErr, Message: err.Error()}
	}
	defer file.Close()

	items, err := m.job.Mapper.Map(m.task.FileName, file)
	if err != nil {
		return types.Result{Code: types.MapErr, Message: err.Error()}
	}

	m.intermediateFiles = make(map[string]storage.File)
	defer func() {
		for _, file := range m.intermediateFiles {
			file.Close()
		}
	}()

	for _, kv := range items {
		if err = m.writeIntermediate(m.task.Index, kv); err != nil {
			return types.Result{Code: types.FileIOErr, Message: err.Error()}
		}
	}

	return types.SucceededResult
}

func (m *mapExecutor) writeIntermediate(mapperIndex int, kv types.KeyValue) error {
	reducerIndex := m.job.Hasher.Hash(kv.Key) % m.job.ReduceNum
	file, err := m.getIntermediateFile(mapperIndex, reducerIndex)
	if err != nil {
		return err
	}
	return json.NewEncoder(file).Encode(&kv)
}

func (m *mapExecutor) getIntermediateFile(mapperIndex int, reducerIndex int) (storage.File, error) {
	fileName := GenerateIntermediateFileName(m.job.Name, mapperIndex, reducerIndex)
	file, exist := m.intermediateFiles[fileName]
	if !exist {
		var err error
		file, err = m.job.Storage.Create(fileName)
		if err != nil {
			return nil, err
		}
		m.intermediateFiles[fileName] = file
	}
	return file, nil
}

func GenerateIntermediateFileName(jobName string, mapperIndex, reducerIndex int) string {
	return fmt.Sprintf("%s-%s-%d-%d", types.MapOutputFilePrefix, jobName, mapperIndex, reducerIndex)
}

func newReduceExecutor(task types.Task, job types.Job) Executor {
	return &reduceExecutor{executor: executor{task: task, job: job}}
}

type reduceExecutor struct {
	executor
}

func (r *reduceExecutor) Run() types.Result {
	kvs, err := r.mergeIntermediateFiles(r.task.Index)
	if err != nil {
		return types.Result{Code: types.FileIOErr, Message: err.Error()}
	}

	result := make([]types.KeyValue, 0, len(kvs))
	for key, values := range kvs {
		value, err := r.job.Reducer.Reduce(key, values)
		if err != nil {
			return types.Result{Code: types.ReduceErr, Message: err.Error()}
		}
		result = append(result, types.KeyValue{Key: key, Value: value})
	}

	if err = r.writeReducerOutput(r.task.Index, result); err != nil {
		return types.Result{Code: types.FileIOErr, Message: err.Error()}
	}

	return types.SucceededResult
}

func (r *reduceExecutor) mergeIntermediateFiles(reducerIndex int) (map[string][]string, error) {
	files := make([]storage.File, 0, r.job.MapNum)
	defer func() {
		for _, file := range files {
			file.Close()
			r.job.Storage.Remove(file.Name())
		}
	}()
	for mapperIndex := 0; mapperIndex < r.job.MapNum; mapperIndex++ {
		file, err := r.job.Storage.Open(GenerateIntermediateFileName(r.job.Name, mapperIndex, reducerIndex))
		if err != nil {
			return nil, err
		}
		files = append(files, file)
	}

	kvs := make(map[string][]string)
	for _, file := range files {
		kvList, err := ReadFile(file)
		if err != nil {
			return nil, err
		}
		for _, kv := range kvList {
			kvs[kv.Key] = append(kvs[kv.Key], kv.Value)
		}
	}

	return kvs, nil
}

func (r *reduceExecutor) writeReducerOutput(reducerIndex int, kvs []types.KeyValue) error {
	file, err := r.job.Storage.Create(GenerateReduceOutputFileName(r.job.Name, reducerIndex))
	if err != nil {
		return err
	}
	defer file.Close()
	encoder := json.NewEncoder(file)
	for _, kv := range kvs {
		encoder.Encode(&kv)
	}
	return nil
}

func GenerateReduceOutputFileName(jobName string, reducerIndex int) string {
	return fmt.Sprintf("%s-%s-%d", types.ReduceOutputFilePrefix, jobName, reducerIndex)
}

func ReadFile(file storage.File) ([]types.KeyValue, error) {
	kv := types.KeyValue{}
	var kvs []types.KeyValue
	decoder := json.NewDecoder(file)
	for {
		err := decoder.Decode(&kv)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		kvs = append(kvs, kv)
	}
	return kvs, nil
}
