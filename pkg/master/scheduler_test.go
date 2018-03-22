package master

import (
	"encoding/json"
	"io/ioutil"
	"math/rand"
	"os"
	"reflect"
	"testing"
	"time"

	"fmt"

	"github.com/mlmhl/mapreduce/pkg/storage"
	"github.com/mlmhl/mapreduce/pkg/types"
	"github.com/mlmhl/mapreduce/pkg/util"
)

func TestMergeReduceOutputFiles(t *testing.T) {
	rand.Seed(time.Now().Unix())
	tmpDir, err := ioutil.TempDir("/tmp", "mr-scheduler-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	s := storage.NewLocalStorage(tmpDir)

	type testCase struct {
		job     types.Job
		targets []types.KeyValue
	}

	prepare := func(c *testCase) {
		files := make([]storage.File, 0, c.job.MapNum)
		defer func() {
			for _, file := range files {
				file.Close()
			}
		}()
		for i := 0; i < c.job.ReduceNum; i++ {
			file, err := c.job.Storage.Create(util.GenerateReduceOutputFileName(c.job.Name, i))
			if err != nil {
				t.Fatal(err)
			}
			files = append(files, file)
		}
		for _, kv := range c.targets {
			index := rand.Intn(c.job.ReduceNum)
			if err := json.NewEncoder(files[index]).Encode(&kv); err != nil {
				t.Fatal(err)
			}
		}
	}

	for _, c := range []testCase{
		{
			job:     types.Job{Name: "test-1", ReduceNum: 1, Storage: s},
			targets: generateKVs(0, 20, 20),
		},
		{
			job:     types.Job{Name: "test-2", ReduceNum: 3, Storage: s},
			targets: generateKVs(0, 20, 20),
		},
		{
			job:     types.Job{Name: "test-3", ReduceNum: 3, Storage: s},
			targets: generateKVs(1, 1, 20),
		},
	} {
		prepare(&c)
		outputFileName, err := mergeReduceOutputFiles(c.job)
		if err != nil {
			t.Fatalf("Test case %s faild as merge error: %v", c.job.Name, err)
		}
		if outputFileName != generateFinalOutputFileName(c.job.Name) {
			t.Fatalf("Test case %s: wrong output file name: expected %s, got %s",
				c.job.Name, generateFinalOutputFileName(c.job.Name), outputFileName)
		}

		file, err := c.job.Storage.Open(outputFileName)
		if err != nil {
			t.Fatalf("Test case %s: Open output file failed: %v", c.job.Name, err)
		}
		kvs, err := util.ReadFile(file)
		if err != nil {
			t.Fatalf("Test case %s: Read output file failed: %v", c.job.Name, err)
		}
		if !reflect.DeepEqual(kvs, c.targets) {
			t.Fatalf("Test case %s: output kvs mismatch: expected %v, got %v", c.job.Name, c.targets, kvs)
		}
	}
}

func generateKVs(start, end, len int) []types.KeyValue {
	val := start
	kvs := make([]types.KeyValue, len)
	for i := 0; i < len; i++ {
		kvs[i] = types.KeyValue{
			Key:   fmt.Sprintf("%3d", val),
			Value: fmt.Sprintf("%3d", val),
		}
		if val < end {
			val++
		}
	}
	return kvs
}
