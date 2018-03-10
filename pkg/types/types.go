package types

import (
	"fmt"
	"hash/fnv"
	"io"
	"time"

	"github.com/mlmhl/mapreduce/pkg/rpc"
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

type ConfigOptions struct {
	// Common options.
	JobName   string
	RootDir   string
	Address   string
	ReduceNum int
	Storage   StorageType

	MasterConfigOptions
	WorkerConfigOptions
}

type MasterConfigOptions struct {
	Mode                 Mode
	InputFiles           []string
	WorkerTimeout        time.Duration
	HealthyCheckInterval time.Duration
}

type WorkerConfigOptions struct {
	MapNum                int
	Limit                 int
	MasterAddress         string
	HealthyReportInterval time.Duration
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
	RpcCallErr
	MapErr
	ReduceErr
	UnknownTask
	ParallelLimitErr
	ExceptionErr
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

type RegisterArg struct {
	Name    string
	Balance int
	Address rpc.Address
}

type RegisterResult struct {
	Result
}

type PingArg struct {
	Name string
}

type WorkerStatus struct {
	Name     string
	Running  int
	Failed   int
	Finished int
}

type MasterConfiguration struct {
	Job        Job
	Mode       Mode
	InputFiles []string

	/*Parameters for parallel mode*/
	Address       rpc.Address
	WorkerTimeout time.Duration
	CheckInterval time.Duration
}

type WorkerConfiguration struct {
	Job                   Job
	Name                  string
	Limit                 int
	Address               rpc.Address
	MasterAddress         rpc.Address
	HealthyReportInterval time.Duration
}
