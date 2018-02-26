package cmd

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"

	"github.com/mlmhl/mapreduce/pkg/master"
	"github.com/mlmhl/mapreduce/pkg/storage"
	"github.com/mlmhl/mapreduce/pkg/types"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "wc",
	Short: "Word counter",
	Long:  `a simple word count example for map reduce framework`,
}

var masterCmd = &cobra.Command{
	Use:   "master (JOB_NAME INPUT_FILES) [flags]",
	Short: "Run as a master",
	Long:  "start a master of the map reduce framework",
	Run: func(cmd *cobra.Command, args []string) {
		var s storage.Storage
		switch options.Storage {
		case types.LocalStorage:
			s = storage.NewLocalStorage(options.RootDir)
		default:
			fmt.Fprintf(os.Stderr, "Unknown storage types: %s. Valid storages: %s\n", options.Storage, types.LocalStorage)
			cmd.Usage()
			return
		}

		job := types.Job{
			Name:      args[0],
			Mapper:    mapper{},
			Reducer:   reducer{},
			Hasher:    types.DefaultHasher(),
			MapNum:    len(args[1:]),
			ReduceNum: options.ReduceNum,
			Storage:   s,
		}

		m, err := master.New(job, options.Mode, args[1:])
		if err != nil {
			fmt.Fprintln(os.Stderr, err.Error())
			cmd.Usage()
			return
		}

		m.Go()
		for {
			if m.Finished() {
				break
			}
			fmt.Printf("%s Status: %+v\n", time.Now(), m.Status())
			time.Sleep(time.Second)
		}
		fmt.Printf("Final Status: %+v\n", m.Status())
	},
}

var options = types.MRConfigOptions{}

func init() {
	rootCmd.AddCommand(masterCmd)

	rootCmd.PersistentFlags().StringVar(&options.RootDir, "root-dir", "/", "Root dir to store input/intermediate/output files")
	rootCmd.PersistentFlags().StringVar(&options.Storage, "storage", types.LocalStorage, fmt.Sprintf("Underlying storage, default: %s", types.LocalStorage))

	masterCmd.PersistentFlags().StringVar(&options.Mode, "mode", types.Sequential, fmt.Sprintf("Execute mode, default: %s", types.Sequential))
	masterCmd.PersistentFlags().IntVar(&options.ReduceNum, "reduce-num", 0, "Reduce number")
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

type mapper struct{}

func (mapper) Map(fileName string, reader io.Reader) ([]types.KeyValue, error) {
	scanner := bufio.NewScanner(reader)
	scanner.Split(bufio.ScanWords)

	var result []types.KeyValue
	for scanner.Scan() {
		result = append(result, types.KeyValue{Key: scanner.Text(), Value: "1"})
	}

	return result, scanner.Err()
}

type reducer struct{}

func (reducer) Reduce(key string, values []string) (string, error) {
	return strconv.Itoa(len(values)), nil
}
