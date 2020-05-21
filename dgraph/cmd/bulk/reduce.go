/*
 * Copyright 2017-2018 Dgraph Labs, Inc. and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package bulk

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"container/heap"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/dgraph-io/badger/v2"
	bo "github.com/dgraph-io/badger/v2/options"
	bpb "github.com/dgraph-io/badger/v2/pb"
	"github.com/dgraph-io/badger/v2/y"
	"github.com/dgraph-io/dgraph/codec"
	"github.com/dgraph-io/dgraph/ee/enc"
	"github.com/dgraph-io/dgraph/posting"
	"github.com/dgraph-io/dgraph/protos/pb"
	"github.com/dgraph-io/dgraph/worker"
	"github.com/dgraph-io/dgraph/x"
	"github.com/gogo/protobuf/proto"
)

type kvBuilder struct {
	currentKey []byte
	uids       []uint64
	pl         *pb.PostingList
	finish     bool
}

type reducer struct {
	*state
	streamId  uint32
	mu        sync.RWMutex
	streamIds map[string]uint32
}

func (r *reducer) run() error {
	dirs := readShardDirs(filepath.Join(r.opt.TmpDir, reduceShardDir))
	x.AssertTrue(len(dirs) == r.opt.ReduceShards)
	x.AssertTrue(len(r.opt.shardOutputDirs) == r.opt.ReduceShards)

	thr := y.NewThrottle(r.opt.NumReducers)
	for i := 0; i < r.opt.ReduceShards; i++ {
		if err := thr.Do(); err != nil {
			return err
		}
		go func(shardId int, db *badger.DB) {
			defer thr.Done(nil)

			mapFiles := filenamesInTree(dirs[shardId])
			var mapItrs []*mapIterator
			for _, mapFile := range mapFiles {
				itr := newMapIterator(mapFile)
				mapItrs = append(mapItrs, itr)
			}

			writer := db.NewStreamWriter()
			x.Check(writer.Prepare())

			ci := &countIndexer{reducer: r, writer: writer}
			r.reduce(mapItrs, ci)
			ci.wait()

			x.Check(writer.Flush())
			for _, itr := range mapItrs {
				if err := itr.Close(); err != nil {
					fmt.Printf("Error while closing iterator: %v", err)
				}
			}
		}(i, r.createBadger(i))
	}
	return thr.Finish()
}

func (r *reducer) createBadger(i int) *badger.DB {
	if r.opt.BadgerKeyFile != "" {
		// Need to set zero addr in WorkerConfig before checking the license.
		x.WorkerConfig.ZeroAddr = []string{r.opt.ZeroAddr}

		if !worker.EnterpriseEnabled() {
			// Crash since the enterprise license is not enabled..
			log.Fatal("Enterprise License needed for the Encryption feature.")
		} else {
			log.Printf("Encryption feature enabled. Using encryption key file: %v", r.opt.BadgerKeyFile)
		}
	}

	opt := badger.DefaultOptions(r.opt.shardOutputDirs[i]).WithSyncWrites(false).
		WithTableLoadingMode(bo.MemoryMap).WithValueThreshold(1 << 10 /* 1 KB */).
		WithLogger(nil).WithMaxCacheSize(1 << 20).
		WithEncryptionKey(enc.ReadEncryptionKeyFile(r.opt.BadgerKeyFile)).WithCompression(bo.None)

	// Overwrite badger options based on the options provided by the user.
	r.setBadgerOptions(&opt)

	db, err := badger.OpenManaged(opt)
	x.Check(err)

	// Zero out the key from memory.
	opt.EncryptionKey = nil

	r.dbs = append(r.dbs, db)
	return db
}

func (r *reducer) setBadgerOptions(opt *badger.Options) {
	// Set the compression level.
	opt.ZSTDCompressionLevel = r.state.opt.BadgerCompressionLevel
	if r.state.opt.BadgerCompressionLevel < 1 {
		x.Fatalf("Invalid compression level: %d. It should be greater than zero",
			r.state.opt.BadgerCompressionLevel)
	}
}

type mapIterator struct {
	fd     *os.File
	reader *bufio.Reader
	tmpBuf []byte
}

func (mi *mapIterator) Close() error {
	return mi.fd.Close()
}

func (mi *mapIterator) Next() *pb.MapEntry {
	r := mi.reader
	buf, err := r.Peek(binary.MaxVarintLen64)
	if err == io.EOF {
		return nil
	}
	x.Check(err)
	sz, n := binary.Uvarint(buf)
	if n <= 0 {
		log.Fatalf("Could not read uvarint: %d", n)
	}
	x.Check2(r.Discard(n))

	for cap(mi.tmpBuf) < int(sz) {
		mi.tmpBuf = make([]byte, sz)
	}
	x.Check2(io.ReadFull(r, mi.tmpBuf[:sz]))

	me := new(pb.MapEntry)
	x.Check(proto.Unmarshal(mi.tmpBuf[:sz], me))
	return me
}

func newMapIterator(filename string) *mapIterator {
	fd, err := os.Open(filename)
	x.Check(err)
	gzReader, err := gzip.NewReader(fd)
	x.Check(err)

	return &mapIterator{fd: fd, reader: bufio.NewReaderSize(gzReader, 16<<10)}
}

func (r *reducer) streamIdFor(pred string) uint32 {
	r.mu.RLock()
	if id, ok := r.streamIds[pred]; ok {
		r.mu.RUnlock()
		return id
	}
	r.mu.RUnlock()
	r.mu.Lock()
	defer r.mu.Unlock()
	if id, ok := r.streamIds[pred]; ok {
		return id
	}
	streamId := atomic.AddUint32(&r.streamId, 1)
	r.streamIds[pred] = streamId
	return streamId
}

func (r *reducer) encodeAndWrite(writer *badger.StreamWriter, entryCh chan []*pb.MapEntry, closer *y.Closer) {
	defer closer.Done()

	var listSize int
	list := &bpb.KVList{}
	//Since a batch doesn't always include all mapEntries with the same key. kvBuilder is needed
	//to save in building kv.
	kvb := &kvBuilder{
		currentKey: nil,
		uids:       nil,
		pl:         new(pb.PostingList),
		finish:     false,
	}
	for batch := range entryCh {
		listSize += r.toList(batch, list, kvb)
		if listSize > 4<<20 {
			for _, kv := range list.Kv {
				pk, err := x.Parse(kv.Key)
				x.Check(err)
				x.AssertTrue(len(pk.Attr) > 0)
				kv.StreamId = r.streamIdFor(pk.Attr)
				if pk.HasStartUid {
					kv.StreamId |= 0x80000000
				}
			}
			x.Check(writer.Write(list))
			list = &bpb.KVList{}
			listSize = 0
		}
	}
	kvb.finish = true
	r.toList(nil, list, kvb)
	if len(list.Kv) > 0 {
		for _, kv := range list.Kv {
			pk, err := x.Parse(kv.Key)
			x.Check(err)
			x.AssertTrue(len(pk.Attr) > 0)
			kv.StreamId = r.streamIdFor(pk.Attr)
			if pk.HasStartUid {
				kv.StreamId |= 0x80000000
			}
		}
		x.Check(writer.Write(list))
	}
}

func (r *reducer) reduce(mapItrs []*mapIterator, ci *countIndexer) {
	entryCh := make(chan []*pb.MapEntry, 100)

	closer := y.NewCloser(1)
	defer closer.SignalAndWait()

	var ph postingHeap
	for _, itr := range mapItrs {
		me := itr.Next()
		if me != nil {
			heap.Push(&ph, heapNode{mapEntry: me, itr: itr})
		} else {
			fmt.Printf("NIL first map entry for %s", itr.fd.Name())
		}
	}

	writer := ci.writer
	go r.encodeAndWrite(writer, entryCh, closer)

	const batchSize = 10000
	const batchAlloc = batchSize * 11 / 10
	batch := make([]*pb.MapEntry, 0, batchAlloc)
	var prevKey []byte
	var plistLen int

	for len(ph.nodes) > 0 {
		node0 := &ph.nodes[0]
		me := node0.mapEntry
		node0.mapEntry = node0.itr.Next()
		if node0.mapEntry != nil {
			heap.Fix(&ph, 0)
		} else {
			heap.Pop(&ph)
		}

		keyChanged := !bytes.Equal(prevKey, me.Key)
		// Note that the keys are coming in sorted order from the heap. So, if
		// we see a new key, we should push out the number of entries we got
		// for the current key, so the count index can register that.
		if keyChanged && plistLen > 0 {
			ci.addUid(prevKey, plistLen)
			plistLen = 0
		}

		//Don't wait keyChanged became true. Sometimes, key doesn't change for a long time.
		//That causes a very large batch size, therefore the memory usage will grow rapaidly.
		//And the process will delay for a long time, therefore slow down average speed.
		if len(batch) >= batchSize {
			entryCh <- batch
			batch = make([]*pb.MapEntry, 0, batchAlloc)
		}
		prevKey = me.Key
		batch = append(batch, me)
		plistLen++
	}
	if len(batch) > 0 {
		entryCh <- batch
	}
	if plistLen > 0 {
		ci.addUid(prevKey, plistLen)
	}
	close(entryCh)
}

type heapNode struct {
	mapEntry *pb.MapEntry
	itr      *mapIterator
}

type postingHeap struct {
	nodes []heapNode
}

func (h *postingHeap) Len() int {
	return len(h.nodes)
}

func (h *postingHeap) Less(i, j int) bool {
	return less(h.nodes[i].mapEntry, h.nodes[j].mapEntry)
}

func (h *postingHeap) Swap(i, j int) {
	h.nodes[i], h.nodes[j] = h.nodes[j], h.nodes[i]
}

func (h *postingHeap) Push(val interface{}) {
	h.nodes = append(h.nodes, val.(heapNode))
}

func (h *postingHeap) Pop() interface{} {
	elem := h.nodes[len(h.nodes)-1]
	h.nodes = h.nodes[:len(h.nodes)-1]
	return elem
}

func (r *reducer) toList(mapEntries []*pb.MapEntry, list *bpb.KVList, kvb *kvBuilder) int {
	//kvBuilder replaces these vars.
	//var currentKey []byte
	//var uids []uint64
	//pl := new(pb.PostingList)
	var size int

	appendToList := func() {
		atomic.AddInt64(&r.prog.reduceKeyCount, 1)

		// For a UID-only posting list, the badger value is a delta packed UID
		// list. The UserMeta indicates to treat the value as a delta packed
		// list when the value is read by dgraph.  For a value posting list,
		// the full pb.Posting type is used (which pb.y contains the
		// delta packed UID list).
		if len(kvb.uids) == 0 {
			return
		}

		// If the schema is of type uid and not a list but we have more than one uid in this
		// list, we cannot enforce the constraint without losing data. Inform the user and
		// force the schema to be a list so that all the data can be found when Dgraph is started.
		// The user should fix their data once Dgraph is up.
		parsedKey, err := x.Parse(kvb.currentKey)
		x.Check(err)
		if parsedKey.IsData() {
			schema := r.state.schema.getSchema(parsedKey.Attr)
			if schema.GetValueType() == pb.Posting_UID && !schema.GetList() && len(kvb.uids) > 1 {
				fmt.Printf("Schema for pred %s specifies that this is not a list but more than  "+
					"one UID has been found. Forcing the schema to be a list to avoid any "+
					"data loss. Please fix the data to your specifications once Dgraph is up.\n",
					parsedKey.Attr)
				r.state.schema.setSchemaAsList(parsedKey.Attr)
			}
		}

		kvb.pl.Pack = codec.Encode(kvb.uids, 256)
		shouldSplit := kvb.pl.Size() > (1<<20)/2 && len(kvb.pl.Pack.Blocks) > 1
		if shouldSplit {
			l := posting.NewList(y.Copy(kvb.currentKey), kvb.pl, r.state.writeTs)
			kvs, err := l.Rollup()
			x.Check(err)
			list.Kv = append(list.Kv, kvs...)
		} else {
			val, err := kvb.pl.Marshal()
			x.Check(err)
			kv := &bpb.KV{
				Key:      y.Copy(kvb.currentKey),
				Value:    val,
				UserMeta: []byte{posting.BitCompletePosting},
				Version:  r.state.writeTs,
			}
			size += kv.Size()
			list.Kv = append(list.Kv, kv)
		}
		kvb.uids = kvb.uids[:0]
		kvb.pl.Reset()
	}

	for _, mapEntry := range mapEntries {
		atomic.AddInt64(&r.prog.reduceEdgeCount, 1)

		if !bytes.Equal(mapEntry.Key, kvb.currentKey) && kvb.currentKey != nil {
			appendToList()
		}
		kvb.currentKey = mapEntry.Key

		uid := mapEntry.Uid
		if mapEntry.Posting != nil {
			uid = mapEntry.Posting.Uid
		}
		if len(kvb.uids) > 0 && kvb.uids[len(kvb.uids)-1] == uid {
			continue
		}
		kvb.uids = append(kvb.uids, uid)
		if mapEntry.Posting != nil {
			kvb.pl.Postings = append(kvb.pl.Postings, mapEntry.Posting)
		}
	}
	if kvb.finish {
		appendToList()
	}
	return size
}
