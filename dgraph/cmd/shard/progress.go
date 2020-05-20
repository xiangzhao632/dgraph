package shard

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/dgraph/x"
)

type phase int32

const (
	nothing phase = iota
	mapPhase
)

type progress struct {
	nquadCount int64
	errCount   int64

	start time.Time

	// shutdown is a bidirectional channel used to manage the stopping of the
	// report goroutine. It handles both the request to stop the report
	// goroutine, as well as the message back to say that the goroutine has
	// stopped. The channel MUST be unbuffered for this to work.
	shutdown chan struct{}

	phase phase
}

func newProgress() *progress {
	return &progress{
		start:    time.Now(),
		shutdown: make(chan struct{}),
	}
}

func (p *progress) setPhase(ph phase) {
	atomic.StoreInt32((*int32)(&p.phase), int32(ph))
}

func (p *progress) report() {
	for {
		select {
		case <-time.After(time.Second):
			p.reportOnce()
		case <-p.shutdown:
			p.shutdown <- struct{}{}
			return
		}
	}
}

func (p *progress) reportOnce() {
	timestamp := time.Now().Format("15:04:05Z0700")
	switch phase(atomic.LoadInt32((*int32)(&p.phase))) {
	case nothing:
	case mapPhase:
		rdfCount := atomic.LoadInt64(&p.nquadCount)
		errCount := atomic.LoadInt64(&p.errCount)
		elapsed := time.Since(p.start)
		fmt.Printf("[%s] MAP %s nquad_count:%s err_count:%s nquad_speed:%s/sec\n",
			timestamp,
			x.FixedDuration(elapsed),
			niceFloat(float64(rdfCount)),
			niceFloat(float64(errCount)),
			niceFloat(float64(rdfCount)/elapsed.Seconds()),
		)
	default:
		x.AssertTruef(false, "invalid phase")
	}
}

func (p *progress) endSummary() {
	p.shutdown <- struct{}{}
	<-p.shutdown

	p.reportOnce()

	total := x.FixedDuration(time.Since(p.start))
	fmt.Printf("Total: %v\n", total)
}

var suffixes = [...]string{"", "k", "M", "G", "T"}

func niceFloat(f float64) string {
	idx := 0
	for f >= 1000 {
		f /= 1000
		idx++
	}
	if idx >= len(suffixes) {
		return fmt.Sprintf("%f", f)
	}
	suf := suffixes[idx]
	switch {
	case f >= 100:
		return fmt.Sprintf("%.1f%s", f, suf)
	case f >= 10:
		return fmt.Sprintf("%.2f%s", f, suf)
	default:
		return fmt.Sprintf("%.3f%s", f, suf)
	}
}
