package util

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"reflect"
	"strconv"
	"testing"

	"github.com/mlmhl/mapreduce/pkg/storage"
	"github.com/mlmhl/mapreduce/pkg/types"
)

func TestMergeIntermediateFiles(t *testing.T) {
	const jobName, keyNum, fileNum = "test-job", 5, 3

	tmpDir, err := ioutil.TempDir("/tmp", "mr-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)
	s := storage.NewLocalStorage(tmpDir)

	executor := reduceExecutor{
		executor: executor{job: types.Job{Name: jobName, MapNum: fileNum, Storage: s}},
	}

	targets := map[string][]string{}
	for value := 0; value < fileNum; value++ {
		file, err := s.Create(GenerateIntermediateFileName(jobName, value, 0))
		if err != nil {
			t.Fatal(err)
		}
		encoder := json.NewEncoder(file)
		for key := 0; key < keyNum; key++ {
			kv := types.KeyValue{Key: strconv.Itoa(key), Value: strconv.Itoa(value)}
			targets[kv.Key] = append(targets[kv.Key], kv.Value)
			if err := encoder.Encode(&kv); err != nil {
				t.Fatal(err)
			}
		}
		file.Close()
	}

	kvs, err := executor.mergeIntermediateFiles(0)
	if err != nil {
		t.Fatalf("Merge failed: %v", err)
	}
	if !reflect.DeepEqual(targets, kvs) {
		t.Fatalf("Wrong intermediate kvs: expected %v, got %v", targets, kvs)
	}
}
