package cmd

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"

	"github.com/mlmhl/mapreduce/pkg/master"
	"github.com/mlmhl/mapreduce/pkg/rpc"
	"github.com/mlmhl/mapreduce/pkg/storage"
	"github.com/mlmhl/mapreduce/pkg/types"
	"github.com/mlmhl/mapreduce/pkg/worker"

	"github.com/spf13/cobra"
	"github.com/mlmhl/mapreduce/pkg/util"
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
		if len(args) < 2 {
			fmt.Fprintf(os.Stderr, "Miss arguments: %v", args)
			cmd.Usage()
			return
		}

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

		config := types.MasterConfiguration{
			Job:        job,
			Mode:       options.Mode,
			InputFiles: args[1:],
		}

		if options.Mode == types.Parallel {
			address, err := rpc.ParseAddress(options.Address)
			if err != nil {
				fmt.Fprintln(os.Stderr, err.Error())
				cmd.Usage()
				return
			}
			config.Address = address
			config.WorkerTimeout = options.WorkerTimeout
			config.CheckInterval = options.HealthyCheckInterval
		}

		m, err := master.New(config)
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

var workerCmd = &cobra.Command{
	Use:   "worker (WORKER_NAME JOB_NAME) [flags]",
	Short: "Run as a worker",
	Long:  "start a worker of the map reduce framework",
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) < 2 {
			fmt.Fprintf(os.Stderr, "Miss arguments: %v", args)
			cmd.Usage()
			return
		}

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
			Name:      args[1],
			Mapper:    mapper{},
			Reducer:   reducer{},
			Hasher:    types.DefaultHasher(),
			MapNum:    options.MapNum,
			ReduceNum: options.ReduceNum,
			Storage:   s,
		}

		address, err := rpc.ParseAddress(options.Address)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Parsing address failed: %v\n", err)
			cmd.Usage()
			return
		}

		masterAddress, err := rpc.ParseAddress(options.MasterAddress)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Parsing master address failed: %v\n", err)
			cmd.Usage()
			return
		}

		config := types.WorkerConfiguration{
			Job:                   job,
			Name:                  args[0],
			Limit:                 options.Limit,
			Address:               address,
			MasterAddress:         masterAddress,
			HealthyReportInterval: options.HealthyReportInterval,
		}

		w, err := worker.New(config)
		if err != nil {
			fmt.Fprintln(os.Stderr, err.Error())
			cmd.Usage()
			return
		}

		w.Go()
		for {
			if w.Finished() {
				break
			}
			fmt.Printf("%s Status: %+v\n", time.Now(), w.Status())
			time.Sleep(time.Second)
		}
		fmt.Printf("Final Status: %+v\n", w.Status())
	},
}

var options = types.ConfigOptions{}

func init() {
	rootCmd.AddCommand(masterCmd)
	rootCmd.AddCommand(workerCmd)

	util.AddGlogFlags(rootCmd.PersistentFlags())

	rootCmd.PersistentFlags().StringVar(&options.RootDir, "root-dir", "/", "Root dir to store input/intermediate/output files")
	rootCmd.PersistentFlags().StringVar(&options.Storage, "storage", types.LocalStorage, fmt.Sprintf("Underlying storage, default: %s", types.LocalStorage))
	rootCmd.PersistentFlags().StringVar(&options.Address, "address", "", "Address to listen, example: tcp:127.0.0.1:8080")
	rootCmd.PersistentFlags().IntVar(&options.ReduceNum, "reduce-num", 0, "Reduce number")


	masterCmd.PersistentFlags().StringVar(&options.Mode, "mode", types.Sequential, fmt.Sprintf("Execute mode, default: %s", types.Sequential))
	masterCmd.PersistentFlags().DurationVar(&options.WorkerTimeout, "worker-timeout", time.Minute, "Maximum time a worker can be lost from master before we remove it, default: 1m")
	masterCmd.PersistentFlags().DurationVar(&options.HealthyCheckInterval, "worker-healthy-check-interval", time.Second*30, "Interval to check whether a worker is timeout or not, default: 30s")

	workerCmd.PersistentFlags().IntVar(&options.MapNum, "map-num", 0, "Map number")
	workerCmd.PersistentFlags().IntVar(&options.Limit, "limit", 1, "Maximum concurrency of running jobs in a single worker, default: 1")
	workerCmd.PersistentFlags().StringVar(&options.MasterAddress, "master-address", "", "Address master listened, example: tcp:127.0.0.1:8080")
	workerCmd.PersistentFlags().DurationVar(&options.HealthyReportInterval, "healthy-report-interval", time.Second*30, "Interval to send a ping signal to master, default: 30s")
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
