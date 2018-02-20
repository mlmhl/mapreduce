package main

import (
	"flag"

	"github.com/mlmhl/mapreduce/examples/word_count/cmd"
)

func main() {
	flag.Parse()
	cmd.Execute()
}
