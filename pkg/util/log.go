package util

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/pflag"
)

// addGlogFlags adds flags from github.com/golang/glog
func AddGlogFlags(fs *pflag.FlagSet) {
	// lookup flags in global flag set and re-register the values with our flagset
	global := flag.CommandLine
	local := pflag.NewFlagSet(os.Args[0], pflag.ExitOnError)

	register(global, local, "logtostderr")
	register(global, local, "alsologtostderr")
	register(global, local, "v")
	register(global, local, "stderrthreshold")
	register(global, local, "vmodule")
	register(global, local, "log_backtrace_at")
	register(global, local, "log_dir")

	fs.AddFlagSet(local)
}

// register adds a flag to local that targets the Value associated with the Flag named globalName in global
func register(global *flag.FlagSet, local *pflag.FlagSet, globalName string) {
	if f := global.Lookup(globalName); f != nil {
		pflagFlag := pflag.PFlagFromGoFlag(f)
		pflagFlag.Name = normalize(pflagFlag.Name)
		local.AddFlag(pflagFlag)
	} else {
		panic(fmt.Sprintf("failed to find flag in global flagset (flag): %s", globalName))
	}
}

// normalize replaces underscores with hyphens
// we should always use hyphens instead of underscores when registering kubelet flags
func normalize(s string) string {
	return strings.Replace(s, "_", "-", -1)
}
