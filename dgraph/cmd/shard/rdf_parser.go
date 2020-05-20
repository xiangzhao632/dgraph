package shard

import (
	"github.com/pkg/errors"
	"strings"
	"unicode/utf8"
)

var (
	// ErrEmpty indicates that the parser encountered a harmless error (e.g empty line or comment).
	ErrEmpty = errors.New("RDF: harmless error, e.g. comment line")
)

type NQuad struct {
	Subject     string
	Predicate   string
	ObjectId    string
	ObjectValue string
}

// ParseRDF parses a mutation string and returns the N-Quad representation for it.
// It parses N-Quad statements based on http://www.w3.org/TR/n-quads/.
func ParseRDF(line string, m *mapper) (*NQuad, error) {
	nq := m.mePool.Get().(*NQuad)
	*nq = NQuad{}

	if !m.opt.NewUids {
		temp := strings.Split(line, " ")
		nq.Subject = temp[0]
		nq.Predicate = temp[1]
		nq.ObjectValue = temp[2]
	}

	if len(line) == 0 {
		m.mePool.Put(nq)
		return nil, ErrEmpty
	}
	/*
		temp := strings.Split(line, " ")
		nq.Subject = temp[0]
		nq.Predicate = temp[1]
		s := strings.Join(temp[2:len(temp)-1], " ")
		if len(s) < 2 {
			return nq, ErrEmpty
		} else if s[0:2] == "_:" {
			nq.ObjectId = s
		} else {
			nq.ObjectValue = s
		}
	*/
	start := 0
	end := 0
	in_string := false
	count := 0
	var temp [3]string
	for count < 3 && end < len(line) {
		d, pos := utf8.DecodeRuneInString(line[end:])
		end += pos
		switch d {
		case ' ', '\t':
			if !in_string {
				//会不会造成line无法释放呢？
				temp[count] = line[start : end-1]
				//temp = append(temp, line[start:end-1])
				start = end
				count++
			}
		case '"':
			in_string = !in_string
		default:
		}
	}
	if count == 3 {
		nq.Subject = temp[0]
		nq.Predicate = temp[1]
		if len(temp[2]) >= 2 && temp[2][0:2] == "_:" {
			nq.ObjectId = temp[2]
		} else {
			nq.ObjectValue = temp[2]
		}
		return nq, nil
	} else {
		m.mePool.Put(nq)
		return nil, errors.Errorf("error while parsing line %s\n", line)
	}
}
