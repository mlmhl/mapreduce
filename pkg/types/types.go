package types

import (
	"fmt"
	"hash/fnv"
	"io"

	"github.com/mlmhl/mapreduce/pkg/storage"
)

type Mode = string

const (
	Parallel   = "Parallel"
	Sequential = "Sequential"
)

var validMode = map[Mode]bool{Parallel: true, Sequential: true}

func ValidaMode(mode Mode) bool {
	return validMode[mode]
}

const (
	MapOutputFilePrefix    = "map-output-tmp"
	ReduceOutputFilePrefix = "reduce-output-tmp"
	FinalOutputFIlePrefix  = "final-output"
)

type KeyValue struct {
	Key   string
	Value string
}

type Mapper interface {
	Map(fileName string, reader io.Reader) ([]KeyValue, error)
}

type Reducer interface {
	Reduce(key string, values []string) (string, error)
}

type Hasher interface {
	Hash(key string) int
}

func DefaultHasher() Hasher {
	return defaultHasher{}
}

type defaultHasher struct{}

func (defaultHasher) Hash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

type StorageType = string

const (
	LocalStorage = "local"
)

type MRConfigOptions struct {
	Mode       Mode
	JobName    string
	MapNum     int
	ReduceNum  int
	InputFiles []string
	RootDir    string
	Storage    StorageType
}

type Job struct {
	Name      string
	Mapper    Mapper
	Reducer   Reducer
	Hasher    Hasher
	MapNum    int
	ReduceNum int
	Storage   storage.Storage
}

type TaskType string

const (
	Map    = "Map"
	Reduce = "Reduce"
)

type TaskState string

const (
	Pending  = "Pending"
	Running  = "Running"
	Finished = "Finished"
)

type Task struct {
	Index    int
	Type     TaskType
	FileName string
}

func (t *Task) Key() string {
	return fmt.Sprintf("%s-%d", t.Type, t.Index)
}

type Result struct {
	Code    int
	Message string
}

var SucceededResult = Result{Code: 0, Message: "Succeeded"}

const (
	FileIOErr = iota
	MapErr
	ReduceErr
	UnknownTask
	ParallelLimitErr
)

type TasksStatus struct {
	Total    int
	Running  int
	Finished int
}

type JobStatus struct {
	Message      string      `json:"message,omitempty"`
	Output       string      `json:"output,omitempty"`
	MapStatus    TasksStatus `json:"mapStatus"`
	ReduceStatus TasksStatus `json:"reduceStatus"`
}
