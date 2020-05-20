package shard

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v2/y"

	"github.com/dgraph-io/dgraph/protos/pb"
	"github.com/dgraph-io/dgraph/x"
	"github.com/dgraph-io/dgraph/xidmap"

	"google.golang.org/grpc"
)

type options struct {
	DataFiles     string
	DataFormat    string
	SchemaFile    string
	OutDir        string
	ReplaceOutDir bool
	TmpDir        string
	NumGoroutines int
	MapBufSize    uint64
	HttpAddr      string
	IgnoreErrors  bool
	Verbose       bool
	ZeroAddr      string
	NumChunker    int
	NewUids       bool

	Weighted     bool
	ReduceShards int
	MapShards    int
	XidShards    int
}

type state struct {
	opt           options
	prog          *progress
	xids          *xidmap.XidMap
	shards        *shardMap
	readerChunkCh chan *bytes.Buffer
	mapFileId     uint32 // Used atomically to name the output files of the mappers.
	writeTs       uint64 // All badger writes use this timestamp
}

type loader struct {
	*state
	mappers []*mapper
	zero    *grpc.ClientConn
}

func newLoader(opt options) *loader {
	fmt.Printf("Connecting to zero at %s\n", opt.ZeroAddr)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	zero, err := grpc.DialContext(ctx, opt.ZeroAddr,
		grpc.WithBlock(),
		grpc.WithInsecure())
	x.Checkf(err, "Unable to connect to zero, Is it running at %s?", opt.ZeroAddr)

	st := &state{
		opt:    opt,
		prog:   newProgress(),
		shards: newShardMap(opt.MapShards),
		// Lots of gz readers, so not much channel buffer needed.
		readerChunkCh: make(chan *bytes.Buffer, 2*opt.NumGoroutines),
		writeTs:       getWriteTimestamp(zero),
	}

	ld := &loader{
		state:   st,
		mappers: make([]*mapper, opt.NumGoroutines),
		zero:    zero,
	}

	for i := 0; i < opt.NumGoroutines; i++ {
		ld.mappers[i] = newMapper(st)
	}
	go ld.prog.report()
	return ld
}

func getWriteTimestamp(zero *grpc.ClientConn) uint64 {
	client := pb.NewZeroClient(zero)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		ts, err := client.Timestamps(ctx, &pb.Num{Val: 1})
		cancel()
		if err == nil {
			return ts.GetStartId()
		}
		fmt.Printf("Error communicating with dgraph zero, retrying: %v", err)
		time.Sleep(time.Second)
	}
}

func (ld *loader) mapStage() {
	ld.prog.setPhase(mapPhase)
	ld.xids = xidmap.New(ld.zero, nil, ld.opt.XidShards, true)

	files := x.FindDataFiles(ld.opt.DataFiles, []string{".rdf", ".rdf.gz", ".json", ".json.gz"})
	if len(files) == 0 {
		fmt.Printf("No data files found in %s.\n", ld.opt.DataFiles)
		os.Exit(1)
	}

	// Because mappers must handle chunks that may be from different input files, they must all
	// assume the same data format, either RDF or JSON. Use the one specified by the user or by
	// the first load file.
	loadType := DataFormat(files[0], ld.opt.DataFormat)
	if loadType == UnknownFormat {
		// Dont't try to detect JSON input in bulk loader.
		fmt.Printf("Need --format=rdf or --format=json to load %s", files[0])
		os.Exit(1)
	}

	var mapperWg sync.WaitGroup
	mapperWg.Add(len(ld.mappers))
	for _, m := range ld.mappers {
		go func(m *mapper) {
			m.run(loadType)
			mapperWg.Done()
		}(m)
	}

	// This is the main map loop.
	thr := y.NewThrottle(ld.opt.NumChunker)
	for i, file := range files {
		x.Check(thr.Do())
		fmt.Printf("Processing file (%d out of %d): %s\n", i+1, len(files), file)

		go func(file string) {
			defer thr.Done(nil)

			r, cleanup := FileReader(file)
			defer cleanup()

			chunker := NewChunker(loadType)
			for {
				chunkBuf, err := chunker.Chunk(r)
				if chunkBuf != nil && chunkBuf.Len() > 0 {
					ld.readerChunkCh <- chunkBuf
				}
				if err == io.EOF {
					break
				} else if err != nil {
					x.Check(err)
				}
			}
		}(file)
	}
	x.Check(thr.Finish())

	close(ld.readerChunkCh)
	mapperWg.Wait()

	// Allow memory to GC before the reduce phase.
	for i := range ld.mappers {
		ld.mappers[i] = nil
	}
	ld.xids = nil
}

func (ld *loader) cleanup() {
	ld.prog.endSummary()
}
