package shard

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/dgraph-io/dgraph/x"
)

// Chunker describes the interface to parse and process the input to the live and bulk loaders.
type Chunker interface {
	Chunk(r *bufio.Reader) (*bytes.Buffer, error)
	Parse(str string, m *mapper) (*NQuad, error)
}

type rdfChunker struct {
}

// InputFormat represents the multiple formats supported by Chunker.
type InputFormat byte

const (
	// UnknownFormat is a constant to denote a format not supported by the bulk/live loaders.
	UnknownFormat InputFormat = iota
	// RdfFormat is a constant to denote the input to the live/bulk loader is in the RDF format.
	RdfFormat
	// JsonFormat is a constant to denote the input to the live/bulk loader is in the JSON format.
	JsonFormat
)

// NewChunker returns a new chunker for the specified format.
func NewChunker(inputFormat InputFormat) Chunker {
	switch inputFormat {
	case RdfFormat:
		return &rdfChunker{}
	default:
		panic("unknown input format")
	}
}

// Chunk reads the input line by line until one of the following 3 conditions happens
// 1) the EOF is reached
// 2) 1e5 lines have been read
// 3) some unexpected error happened
func (*rdfChunker) Chunk(r *bufio.Reader) (*bytes.Buffer, error) {
	batch := new(bytes.Buffer)
	batch.Grow(1 << 20)
	for lineCount := 0; lineCount < 1e5; lineCount++ {
		slc, err := r.ReadSlice('\n')
		if err == io.EOF {
			if _, err := batch.Write(slc); err != nil {
				return nil, err
			}
			return batch, err
		}
		if err == bufio.ErrBufferFull {
			// This should only happen infrequently.
			if _, err := batch.Write(slc); err != nil {
				return nil, err
			}
			var str string
			str, err = r.ReadString('\n')
			if err == io.EOF {
				if _, err := batch.WriteString(str); err != nil {
					return nil, err
				}
				return batch, err
			}
			if err != nil {
				return nil, err
			}
			if _, err := batch.WriteString(str); err != nil {
				return nil, err
			}
			continue
		}
		if err != nil {
			return nil, err
		}
		if _, err := batch.Write(slc); err != nil {
			return nil, err
		}
	}
	return batch, nil
}

// Parse is not thread-safe. Only call it serially, because it reuses lexer object.
func (rc *rdfChunker) Parse(str string, m *mapper) (*NQuad, error) {
	nq, err := ParseRDF(str, m)
	return nq, err
}

// FileReader returns an open reader and file on the given file. Gzip-compressed input is detected
// and decompressed automatically even without the gz extension. The caller is responsible for
// calling the returned cleanup function when done with the reader.
func FileReader(file string) (rd *bufio.Reader, cleanup func()) {
	var f *os.File
	var err error
	if file == "-" {
		f = os.Stdin
	} else {
		f, err = os.Open(file)
	}

	x.Check(err)

	cleanup = func() { _ = f.Close() }

	if filepath.Ext(file) == ".gz" {
		gzr, err := gzip.NewReader(f)
		x.Check(err)
		rd = bufio.NewReader(gzr)
		cleanup = func() { _ = f.Close(); _ = gzr.Close() }
	} else {
		rd = bufio.NewReader(f)
		buf, _ := rd.Peek(512)

		typ := http.DetectContentType(buf)
		if typ == "application/x-gzip" {
			gzr, err := gzip.NewReader(rd)
			x.Check(err)
			rd = bufio.NewReader(gzr)
			cleanup = func() { _ = f.Close(); _ = gzr.Close() }
		}
	}

	return rd, cleanup
}

// DataFormat returns a file's data format (RDF, JSON, or unknown) based on the filename
// or the user-provided format option. The file extension has precedence.
func DataFormat(filename string, format string) InputFormat {
	format = strings.ToLower(format)
	filename = strings.TrimSuffix(strings.ToLower(filename), ".gz")
	switch {
	case strings.HasSuffix(filename, ".rdf") || format == "rdf":
		return RdfFormat
	case strings.HasSuffix(filename, ".json") || format == "json":
		return JsonFormat
	default:
		return UnknownFormat
	}
}
