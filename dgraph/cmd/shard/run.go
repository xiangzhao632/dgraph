package shard

import (
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net/http"
	_ "net/http/pprof" // http profiler
	"os"
	"runtime"
	"strings"

	"github.com/dgraph-io/dgraph/x"
	"github.com/spf13/cobra"
)

// Shard is the sub-command invoked when running "dgraph Shard".
var Shard x.SubCommand

func init() {
	Shard.Cmd = &cobra.Command{
		Use:   "shard",
		Short: "Run Dgraph shard",
		Run: func(cmd *cobra.Command, args []string) {
			defer x.StartProfile(Shard.Conf).Stop()
			run()
		},
	}
	Shard.EnvPrefix = "DGRAPH_SHARD"

	flag := Shard.Cmd.Flags()
	flag.StringP("files", "f", "",
		"Location of *.rdf(.gz) or *.json(.gz) file(s) to load.")
	flag.StringP("schema", "s", "",
		"Location of schema file.")
	flag.String("format", "",
		"Specify file format (rdf or json) instead of getting it from filename.")
	flag.String("tmp", "split_output",
		"Temp directory used to use for on-disk scratch space. Requires free space proportional"+
			" to the size of the RDF file and the amount of indexing used.")
	flag.IntP("num_go_routines", "j", int(math.Ceil(float64(runtime.NumCPU())/2.0)),
		"Number of worker threads to use. MORE THREADS LEAD TO HIGHER RAM USAGE.")
	flag.Int64("mapoutput_mb", 64,
		"The estimated size of each map file output. Increasing this increases memory usage.")
	flag.StringP("zero", "z", "localhost:5080", "gRPC address for Dgraph zero")

	// TODO: Potentially move http server to main.
	flag.String("http", "localhost:8080",
		"Address to serve http (pprof).")
	flag.Bool("ignore_errors", true, "ignore line parsing errors in rdf files")
	flag.Bool("verbose", false, "output details to debug")
	flag.Bool("new_uids", false,
		"Ignore UIDs in load files and assign new ones.")
	flag.Int("map_shards", 1,
		"Number of map output shards. Must be greater than or equal to the number of reduce "+
			"shards. Increasing allows more evenly sized reduce shards, at the expense of "+
			"increased memory usage. 0 for no limit.")

	flag.Bool("weighted", true,
		"Add weight to each predicate.")
	flag.Int("reduce_shards", 1,
		"Number of reduce shards. This determines the number of dgraph instances in the final "+
			"cluster. Increasing this potentially decreases the reduce stage runtime by using "+
			"more parallelism, but increases memory usage.")
	flag.Int("xid_shards", 32,
		"Ignore UIDs in load files and assign new ones.")
	flag.Int("chunkers", 1,
		"Number of chunkers. This determines the number of goroutines chunk rdf files")
}

func run() {
	opt := options{
		DataFiles:     Shard.Conf.GetString("files"),
		DataFormat:    Shard.Conf.GetString("format"),
		SchemaFile:    Shard.Conf.GetString("schema"),
		TmpDir:        Shard.Conf.GetString("tmp"),
		NumGoroutines: Shard.Conf.GetInt("num_go_routines"),
		MapBufSize:    uint64(Shard.Conf.GetInt("mapoutput_mb")),
		HttpAddr:      Shard.Conf.GetString("http"),
		IgnoreErrors:  Shard.Conf.GetBool("ignore_errors"),
		Verbose:       Shard.Conf.GetBool("verbose"),
		MapShards:     Shard.Conf.GetInt("map_shards"),
		ReduceShards:  Shard.Conf.GetInt("reduce_shards"),
		ZeroAddr:      Shard.Conf.GetString("zero"),
		NewUids:       Shard.Conf.GetBool("new_uids"),

		XidShards:  Shard.Conf.GetInt("xid_shards"),
		NumChunker: Shard.Conf.GetInt("chunkers"),
		Weighted:   Shard.Conf.GetBool("weighted"),
	}
	x.PrintVersion()

	if opt.SchemaFile == "" {
		fmt.Fprint(os.Stderr, "Schema file must be specified.\n")
		os.Exit(1)
	} else if _, err := os.Stat(opt.SchemaFile); err != nil && os.IsNotExist(err) {
		fmt.Fprintf(os.Stderr, "Schema path(%v) does not exist.\n", opt.SchemaFile)
		os.Exit(1)
	}

	if opt.DataFiles == "" {
		fmt.Fprint(os.Stderr, "RDF or JSON file(s) location must be specified.\n")
		os.Exit(1)
	} else {
		fileList := strings.Split(opt.DataFiles, ",")
		for _, file := range fileList {
			if _, err := os.Stat(file); err != nil && os.IsNotExist(err) {
				fmt.Fprintf(os.Stderr, "Data path(%v) does not exist.\n", file)
				os.Exit(1)
			}
		}
	}

	opt.MapBufSize <<= 20 // Convert from MB to B.

	optBuf, err := json.MarshalIndent(&opt, "", "\t")
	x.Check(err)
	fmt.Println(string(optBuf))

	go func() {
		log.Fatal(http.ListenAndServe(opt.HttpAddr, nil))
	}()

	loader := newLoader(opt)
	loader.mapStage()
	loader.mergeMapShardsIntoReduceShards()

	loader.cleanup()
}
