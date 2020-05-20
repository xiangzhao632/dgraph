package shard

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/dgraph-io/dgraph/x"
)

const (
	mapShardDir    = "map_shards"
	reduceShardDir = "reduce_shards"
)

func (ld *loader) mergeMapShardsIntoReduceShards() {
	map2reduce := map[string]string{}
	pred2Schema := map[string]string{}
	schemaFile := map[string]*bufio.Writer{}
	var temp []string
	//read schema file
	s, err := os.Open(ld.opt.SchemaFile)
	if err != nil {
		return
	}
	defer s.Close()
	scanner := bufio.NewScanner(s)
	for scanner.Scan() {
		line := scanner.Text()
		temp = strings.Split(line, ":")
		pred := fmt.Sprintf("<%s>", strings.TrimSpace(temp[0]))
		pred2Schema[pred] = line
	}
	////
	ld.shards.statPred()
	var shardDirs []sizedDir
	if ld.opt.Weighted {
		shardDirs = getWeightedDirs(ld, pred2Schema)
	} else {
		shardDirs = readSize(readShardDirs(filepath.Join(ld.opt.TmpDir, mapShardDir)))
	}
	if len(shardDirs) == 0 {
		fmt.Printf(
			"No map shards found. Possibly caused by empty data files passed to the bulk loader or wrong data format.\n")
		os.Exit(1)
	}

	sort.SliceStable(shardDirs, func(i, j int) bool {
		return shardDirs[i].sz > shardDirs[j].sz
	})
	////
	var reduceShards []sizedDir
	for i := 0; i < ld.opt.ReduceShards; i++ {
		shardDir := filepath.Join(ld.opt.TmpDir, reduceShardDir, fmt.Sprintf("shard_%d", i))
		x.Check(os.MkdirAll(shardDir, 0750))
		reduceShards = append(reduceShards, sizedDir{dir: shardDir, sz: 0})

		f, err := os.OpenFile(filepath.Join(shardDir, "schema"), os.O_CREATE|os.O_WRONLY, 0600)
		if err != nil {
			return
		}
		defer f.Close()
		schemaFile[fmt.Sprintf("shard_%d", i)] = bufio.NewWriter(f)
	}

	// Heuristic: put the largest map shard into the smallest reduce shard
	// until there are no more map shards left. Should be a good approximation.

	for _, sizedDir := range shardDirs {
		sort.SliceStable(reduceShards, func(i, j int) bool {
			return reduceShards[i].sz > reduceShards[j].sz
		})
		reduceShard := filepath.Join(reduceShards[len(reduceShards)-1].dir, filepath.Base(sizedDir.dir))
		reduceShards[len(reduceShards)-1].sz += sizedDir.sz
		fmt.Printf("MapShard %s -> ReduceShard %s\n", sizedDir.dir, reduceShard)

		temp = strings.Split(reduceShard, "/")
		map2reduce[temp[len(temp)-1]] = temp[len(temp)-2]

		x.Check(os.Rename(sizedDir.dir, reduceShard))
	}

	//处理schema文件有，但数据文件没有的谓词
	count := 0
	var reduceStr string
	for pred, schema := range pred2Schema {
		if !ld.shards.has(pred) {
			reduceStr = fmt.Sprintf("shard_%d", count%ld.opt.ReduceShards)
			count++
		} else {
			mapId := ld.shards.shardFor(pred)
			reduceStr = map2reduce[fmt.Sprintf("%03d", mapId)]
		}
		schemaFile[reduceStr].WriteString(schema)
		schemaFile[reduceStr].WriteRune('\n')
	}
	for _, f := range schemaFile {
		f.Flush()
	}

	var typeShard string
	if ld.shards.has("<dgraph.type>") {
		mapId := ld.shards.shardFor("<dgraph.type>")
		reduceStr := map2reduce[fmt.Sprintf("%03d", mapId)]
		typeShard = reduceStr[len("shard_"):]
	} else {
		typeShard = strconv.Itoa(count % ld.opt.ReduceShards)
	}

	typeFile := filepath.Join(ld.opt.TmpDir, "dgraph_type")
	t, err := os.OpenFile(typeFile, os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return
	}
	defer t.Close()
	if _, err := t.WriteString(typeShard); err != nil {
		return
	}
	if _, err := t.WriteString("\n"); err != nil {
		return
	}
}

func readShardDirs(d string) []string {
	_, err := os.Stat(d)
	if os.IsNotExist(err) {
		return nil
	}
	dir, err := os.Open(d)
	x.Check(err)
	shards, err := dir.Readdirnames(0)
	x.Check(err)
	x.Check(dir.Close())
	for i, shard := range shards {
		shards[i] = filepath.Join(d, shard)
	}
	sort.Strings(shards)
	return shards
}

func readSize(dirs []string) []sizedDir {
	sizedDirs := make([]sizedDir, len(dirs))
	for _, dir := range dirs {
		sizedDirs = append(sizedDirs, sizedDir{dir: dir, sz: treeSize(dir)})
	}
	return sizedDirs
}

func getWeightedDirs(ld *loader, pred2Schema map[string]string) []sizedDir {
	sizedDirs := []sizedDir{}
	temp := map[string]int64{}
	for pred, mapId := range ld.shards.predToShard {
		schema := pred2Schema[pred]
		dir := filepath.Join(ld.opt.TmpDir, mapShardDir, fmt.Sprintf("%03d", mapId))
		_, ok := temp[dir]
		if ok {
			temp[dir] += treeSize(dir) * getWeight(schema)
		} else {
			temp[dir] = treeSize(dir) * getWeight(schema)
		}
	}
	for dir, size := range temp {
		sizedDirs = append(sizedDirs, sizedDir{dir: dir, sz: size})
	}
	return sizedDirs
}

func getWeight(schema string) int64 {
	var weight int64 = 1
	if strings.Contains(schema, "hash") {
		weight += 1
	}
	if strings.Contains(schema, "exact") {
		weight += 2
	}
	if strings.Contains(schema, "term") {
		weight += 3
	}
	if strings.Contains(schema, "fulltext") {
		weight += 4
	}
	if strings.Contains(schema, "trigram") {
		weight += 5
	}
	return weight
}

func filenamesInTree(dir string) []string {
	var fnames []string
	x.Check(filepath.Walk(dir, func(path string, fi os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if strings.HasSuffix(path, ".gz") {
			fnames = append(fnames, path)
		}
		return nil
	}))
	return fnames
}

type sizedDir struct {
	dir string
	sz  int64
}

// sortBySize sorts the input directories by size of their content (biggest to smallest).
func sortBySize(dirs []string) {
	sizedDirs := make([]sizedDir, len(dirs))
	for i, dir := range dirs {
		sizedDirs[i] = sizedDir{dir: dir, sz: treeSize(dir)}
	}
	sort.SliceStable(sizedDirs, func(i, j int) bool {
		return sizedDirs[i].sz > sizedDirs[j].sz
	})
	for i := range sizedDirs {
		dirs[i] = sizedDirs[i].dir
	}
}

func treeSize(dir string) int64 {
	var sum int64
	x.Check(filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		sum += info.Size()
		return nil
	}))
	return sum
}
