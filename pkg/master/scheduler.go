package master

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"

	"github.com/mlmhl/mapreduce/pkg/storage"
	"github.com/mlmhl/mapreduce/pkg/types"
)

type scheduler interface {
	Run() (string, error)
}

type schedulerImpl interface {
	runOne(task types.Task) types.Result
}

func newGenericScheduler(
	meta types.MetaData,
	manager taskManager,
	storage storage.Storage) genericScheduler {
	return genericScheduler{
		meta:    meta,
		manager: manager,
		storage: storage,
	}
}

type genericScheduler struct {
	impl schedulerImpl

	meta    types.MetaData
	manager taskManager
	storage storage.Storage
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

func (s *genericScheduler) doMap(task types.Task) types.Result {
	file, err := s.storage.Open(task.FileName)
	if err != nil {
		return types.Result{Code: types.FileIOErr, Message: err.Error()}
	}
	defer file.Close()

	items, err := s.meta.Mapper.Map(task.FileName, file)
	if err != nil {
		return types.Result{Code: types.MapErr, Message: err.Error()}
	}

	intermediateFiles := make(map[string]storage.File)
	defer func() {
		for _, file := range intermediateFiles {
			file.Close()
		}
	}()

	for _, kv := range items {
		if err = s.writeIntermediate(intermediateFiles, task.Index, kv); err != nil {
			return types.Result{Code: types.FileIOErr, Message: err.Error()}
		}
	}

	return types.SucceededResult
}

func (s *genericScheduler) writeIntermediate(
	intermediateFiles map[string]storage.File,
	mapperIndex int, kv types.KeyValue) error {
	reducerIndex := s.meta.Hasher.Hash(kv.Key) % s.meta.ReduceNum
	file, err := s.getIntermediateFile(intermediateFiles, mapperIndex, reducerIndex)
	if err != nil {
		return err
	}
	return json.NewEncoder(file).Encode(&kv)
}

func (s *genericScheduler) getIntermediateFile(
	intermediateFiles map[string]storage.File,
	mapperIndex, reducerIndex int) (storage.File, error) {
	fileName := generateIntermediateFileName(s.meta.Name, mapperIndex, reducerIndex)
	file, exist := intermediateFiles[fileName]
	if !exist {
		var err error
		file, err = s.storage.Create(fileName)
		if err != nil {
			return nil, err
		}
		intermediateFiles[fileName] = file
	}
	return file, nil
}

func generateIntermediateFileName(jobName string, mapperIndex, reducerIndex int) string {
	return fmt.Sprintf("%s-%s-%d-%d", types.MapOutputFilePrefix, jobName, mapperIndex, reducerIndex)
}

func (s *genericScheduler) doReduce(task types.Task) types.Result {
	kvs, err := s.mergeIntermediateFiles(task.Index)
	if err != nil {
		return types.Result{Code: types.FileIOErr, Message: err.Error()}
	}

	result := make([]types.KeyValue, 0, len(kvs))
	for key, values := range kvs {
		value, err := s.meta.Reducer.Reduce(key, values)
		if err != nil {
			return types.Result{Code: types.ReduceErr, Message: err.Error()}
		}
		result = append(result, types.KeyValue{Key: key, Value: value})
	}

	if err = s.writeReducerOutput(task.Index, result); err != nil {
		return types.Result{Code: types.FileIOErr, Message: err.Error()}
	}

	return types.SucceededResult
}

func (s *genericScheduler) mergeIntermediateFiles(reducerIndex int) (map[string][]string, error) {
	files := make([]storage.File, 0, s.meta.MapNum)
	defer func() {
		for _, file := range files {
			file.Close()
			s.storage.Remove(file.Name())
		}
	}()
	for mapperIndex := 0; mapperIndex < s.meta.MapNum; mapperIndex++ {
		file, err := s.storage.Open(generateIntermediateFileName(s.meta.Name, mapperIndex, reducerIndex))
		if err != nil {
			return nil, err
		}
		files = append(files, file)
	}

	kvs := make(map[string][]string)
	for _, file := range files {
		kvList, err := readFile(file)
		if err != nil {
			return nil, err
		}
		for _, kv := range kvList {
			kvs[kv.Key] = append(kvs[kv.Key], kv.Value)
		}
	}

	return kvs, nil
}

func (s *genericScheduler) writeReducerOutput(reducerIndex int, kvs []types.KeyValue) error {
	file, err := s.storage.Create(generateReduceOutputFileName(s.meta.Name, reducerIndex))
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

func generateReduceOutputFileName(jobName string, reducerIndex int) string {
	return fmt.Sprintf("%s-%s-%d", types.ReduceOutputFilePrefix, jobName, reducerIndex)
}

func (s *genericScheduler) mergeReduceOutputFiles() (string, error) {
	files := make([]string, s.meta.ReduceNum)
	defer func() {
		for _, fileName := range files {
			s.storage.Remove(fileName)
		}
	}()

	var kvs []types.KeyValue

	for i := 0; i < s.meta.ReduceNum; i++ {
		fileName := generateReduceOutputFileName(s.meta.Name, i)
		file, err := s.storage.Open(fileName)
		if err != nil {
			return "", err
		}
		files[i] = fileName

		kvList, err := readFile(file)
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

	outputFileName := generateFinalOutputFileName(s.meta.Name)
	file, err := s.storage.Create(outputFileName)
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

func readFile(file storage.File) ([]types.KeyValue, error) {
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

func newSequentialScheduler(
	meta types.MetaData,
	manager taskManager,
	storage storage.Storage) scheduler {
	s := &sequentialScheduler{
		genericScheduler: newGenericScheduler(meta, manager, storage),
	}
	s.impl = s
	return s
}

type sequentialScheduler struct {
	genericScheduler
}

func (s *sequentialScheduler) runOne(task types.Task) types.Result {
	var result types.Result

	switch task.Type {
	case types.Map:
		result = s.doMap(task)
	case types.Reduce:
		result = s.doReduce(task)
	default:
		result = types.Result{Code: types.UnknownTask, Message: fmt.Sprintf("Unknown task type: %s", task.Type)}
	}

	return result
}
