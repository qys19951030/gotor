// internal/crawl/frontier.go
package crawl

import (
	"sync"
)

type Frontier struct {
	ch             chan Task
	mu             sync.Mutex
	seen           map[string]struct{}
	resumed        map[string]struct{}
	skippedHistory int
	inflight       int
	pending        int
	closed         bool
	once           sync.Once
}

func NewFrontier(size int) *Frontier {
	return &Frontier{
		ch:      make(chan Task, size),
		seen:    make(map[string]struct{}),
		resumed: make(map[string]struct{}),
	}
}

// MarkSeen records a URL as already crawled without enqueueing it, so a
// resumed run dedupes with exactly the same semantics as a fresh run.
func (f *Frontier) MarkSeen(key string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.seen[key]; ok {
		return
	}
	f.seen[key] = struct{}{}
	f.resumed[key] = struct{}{}
}

// SkippedHistory reports how many distinct URLs were not enqueued because
// the resumed history already contains them.
func (f *Frontier) SkippedHistory() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.skippedHistory
}

func (f *Frontier) EnqueueIfNew(t Task) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return false
	}
	key := t.URL.String()
	if _, ok := f.seen[key]; ok {
		if _, ok := f.resumed[key]; ok {
			// Count each historically crawled URL once, the first time a
			// link to it is rejected.
			delete(f.resumed, key)
			f.skippedHistory++
		}
		return false
	}

	select {
	case f.ch <- t:
		f.seen[key] = struct{}{}
		f.pending++
		return true
	default:
		return false
	}
}

func (f *Frontier) Next() <-chan Task { return f.ch }

func (f *Frontier) markStart() {
	f.mu.Lock()
	f.inflight++
	f.mu.Unlock()
}

func (f *Frontier) markDone() {
	f.mu.Lock()
	f.inflight--
	f.pending--
	f.mu.Unlock()
}

func (f *Frontier) Stats() (queued, inflight, pending int, closed bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.ch), f.inflight, f.pending, f.closed
}

func (f *Frontier) CloseOnce() {
	f.once.Do(func() {
		f.mu.Lock()
		f.closed = true
		close(f.ch)
		f.mu.Unlock()
	})
}
