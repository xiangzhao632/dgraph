package shard

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"github.com/pkg/errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"unicode/utf8"

	//"unicode/utf8"

	"github.com/dgraph-io/dgraph/x"
)

type mapper struct {
	*state
	shards []shardState // shard is based on predicate
	sb     strings.Builder
	mePool *sync.Pool
}

type shardState struct {
	// Buffer up map entries until we have a sufficient amount, then sort and
	// write them to file.
	entries     []string
	encodedSize uint64
	mu          sync.Mutex // Allow only 1 write per shard at a time.
}

func newMapper(st *state) *mapper {
	return &mapper{
		state:  st,
		shards: make([]shardState, st.opt.MapShards),
		sb:     strings.Builder{},
		mePool: &sync.Pool{
			New: func() interface{} {
				return &NQuad{}
			},
		},
	}
}

func (m *mapper) openOutputFile(shardIdx int) (*os.File, error) {
	fileNum := atomic.AddUint32(&m.mapFileId, 1)
	filename := filepath.Join(
		m.opt.TmpDir,
		mapShardDir,
		fmt.Sprintf("%03d", shardIdx),
		//fmt.Sprintf("%06d.rdf.gz", fileNum),
		fmt.Sprintf("%06d.rdf.gz", fileNum),
	)
	x.Check(os.MkdirAll(filepath.Dir(filename), 0750))
	return os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
}

func (m *mapper) writeMapEntriesToFile(entries []string, encodedSize uint64, shardIdx int) {
	defer m.shards[shardIdx].mu.Unlock() // Locked by caller.

	f, err := m.openOutputFile(shardIdx)
	x.Check(err)

	defer func() {
		x.Check(f.Sync())
		x.Check(f.Close())
	}()
	gzWriter := gzip.NewWriter(f)
	w := bufio.NewWriter(gzWriter)
	//w := bufio.NewWriter(f)
	defer func() {
		x.Check(w.Flush())
		x.Check(gzWriter.Flush())
		x.Check(gzWriter.Close())
	}()

	for _, me := range entries {
		w.WriteString(me)
		//w.WriteRune('\n')
	}
}

func (m *mapper) run(inputFormat InputFormat) {

	for chunkBuf := range m.readerChunkCh {
		for chunkBuf.Len() > 0 {
			str, err := chunkBuf.ReadString('\n')
			if err != nil && err != io.EOF {
				x.Check(err)
			}
			err = m.processNQuad(str)
			if err == ErrEmpty {
				continue
			} else if err != nil {
				atomic.AddInt64(&m.prog.errCount, 1)
				if !m.opt.IgnoreErrors {
					x.Check(err)
				}
				if m.opt.Verbose {
					fmt.Printf("Illegal RDF: %s\n", str)
				}
			} else {
				atomic.AddInt64(&m.prog.nquadCount, 1)
			}
		}
		for i := range m.shards {
			sh := &m.shards[i]
			if sh.encodedSize >= m.opt.MapBufSize {
				sh.mu.Lock() // One write at a time.
				go m.writeMapEntriesToFile(sh.entries, sh.encodedSize, i)
				// Clear the entries and encodedSize for the next batch.
				// Proactively allocate 32 slots to bootstrap the entries slice.
				sh.entries = make([]string, 0, 512)
				sh.encodedSize = 0
			}
		}
	}

	for i := range m.shards {
		sh := &m.shards[i]
		if len(sh.entries) > 0 {
			sh.mu.Lock() // One write at a time.
			m.writeMapEntriesToFile(sh.entries, sh.encodedSize, i)
		}
		m.shards[i].mu.Lock() // Ensure that the last file write finishes.
	}
}

func (m *mapper) addMapEntry(rdf string, shard int) {

	sh := &m.shards[shard]

	var err error
	sh.entries = append(sh.entries, rdf)
	sh.encodedSize += uint64(len(rdf))
	x.Check(err)
}

func (m *mapper) processNQuad(line string) error {
	if !m.opt.NewUids {
		temp := strings.Split(line, " ")
		shard := m.state.shards.shardFor(temp[1])
		m.addMapEntry(line, shard)
		return nil
	}
	if len(line) == 0 {
		return ErrEmpty
	}

	start := 0
	end := 0
	in_string := false
	count := 0
	var temp = make([]string, 4)
	for count < 3 && end < len(line) {
		d, pos := utf8.DecodeRuneInString(line[end:])
		end += pos
		switch d {
		case ' ', '\t':
			if !in_string {
				//会不会造成line无法释放呢？
				temp[count] = line[start : end-1]
				switch count {
				case 0:
					id := m.uid(temp[0])
					temp[0] = fmt.Sprintf("<0x%s>", strconv.FormatUint(id, 16))
				case 2:
					if len(temp[2]) > 2 && temp[2][0:2] == "_:" {
						id := m.uid(temp[2])
						temp[2] = fmt.Sprintf("<0x%s>", strconv.FormatUint(id, 16))
					}
				default:
				}
				start = end
				count++
			}
		case '"':
			in_string = !in_string
		default:
		}
	}
	if count == 3 {
		shard := m.state.shards.shardFor(temp[1])
		if end < len(line) {
			temp[3] = line[start:]
		}
		line = strings.Join(temp, " ")
		m.addMapEntry(line, shard)
		return nil
	} else {
		return errors.Errorf("error while parsing line %s\n", line)
	}
}

func (m *mapper) uid(xid string) uint64 {

	return m.lookupUid(xid)
}

func (m *mapper) lookupUid(xid string) uint64 {
	// We create a copy of xid string here because it is stored in
	// the map in AssignUid and going to be around throughout the process.
	// We don't want to keep the whole line that we read from file alive.
	// xid is a substring of the line that we read from the file and if
	// xid is alive, the whole line is going to be alive and won't be GC'd.
	// Also, checked that sb goes on the stack whereas sb.String() goes on
	// heap. Note that the calls to the strings.Builder.* are inlined.
	//sb := strings.Builder{}
	m.sb.WriteString(xid)
	uid, _ := m.xids.AssignUid(m.sb.String())
	m.sb.Reset()
	return uid
}
