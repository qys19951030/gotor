// internal/crawl/journal.go
package crawl

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Record statuses written to the crawl journal.
const (
	RecordStatusSuccess = "success"
	RecordStatusFailed  = "failed"
)

// Record is one append-only journal line describing a single fetched page.
// Records are written as soon as a page finishes so an interrupted crawl
// still leaves a complete, parseable history on disk.
type Record struct {
	URL        string    `json:"url"`
	ParentURL  string    `json:"parentUrl,omitempty"`
	Depth      int       `json:"depth"`
	Status     string    `json:"status"`
	DurationMS int64     `json:"durationMs"`
	FetchedAt  time.Time `json:"fetchedAt"`
	Error      string    `json:"error,omitempty"`
}

// journalFileName is the append-only record log inside a record directory.
const journalFileName = "records.jsonl"

// defaultFlushEvery bounds how many records are buffered before they are
// flushed to disk as one chunk.
const defaultFlushEvery = 16

// hostCounts tracks how many pages of one site the journal has seen.
type hostCounts struct {
	Success int
	Failed  int
}

// history is the resume state rebuilt by streaming an existing journal.
type history struct {
	succeeded    map[string]struct{}   // URLs whose last record is a success
	failed       []Record              // latest failed record per URL
	successTotal int                   // cumulative successful records
	failedTotal  int                   // cumulative failed records
	hosts        map[string]hostCounts // per-site crawled counts
}

// apply folds one record into the resume state. The last record per URL
// decides whether the URL is treated as done or is queued again.
func (h *history) apply(record Record) {
	if record.Status == RecordStatusSuccess {
		h.successTotal++
	} else {
		h.failedTotal++
	}

	host := recordHost(record.URL)
	counts := h.hosts[host]
	if record.Status == RecordStatusSuccess {
		counts.Success++
	} else {
		counts.Failed++
	}
	h.hosts[host] = counts

	if record.Status == RecordStatusSuccess {
		h.succeeded[record.URL] = struct{}{}
		for i, queued := range h.failed {
			if queued.URL == record.URL {
				h.failed = append(h.failed[:i], h.failed[i+1:]...)
				break
			}
		}
		return
	}

	if _, ok := h.succeeded[record.URL]; ok {
		// A failure recorded after a success does not resurrect the page:
		// the successful fetch remains the final state.
		return
	}
	for i, queued := range h.failed {
		if queued.URL == record.URL {
			h.failed[i] = record
			return
		}
	}
	h.failed = append(h.failed, record)
}

func recordHost(rawURL string) string {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return ""
	}
	return parsed.Hostname()
}

// validateRecord rejects lines that decode but cannot describe a crawl
// outcome, so corrupt journals fail before the crawl starts.
func validateRecord(record Record) error {
	if record.URL == "" {
		return errors.New("record is missing a URL")
	}
	if record.Status != RecordStatusSuccess && record.Status != RecordStatusFailed {
		return fmt.Errorf("record %q has unknown status %q", record.URL, record.Status)
	}
	return nil
}

// loadHistory streams journal lines from reader without loading the whole
// file into memory. A truncated final line (process killed mid-write) is
// dropped and reported through the dropped counter; any other unparsable
// content is an error.
func loadHistory(reader io.Reader) (history, int, error) {
	state := history{
		succeeded: make(map[string]struct{}),
		hosts:     make(map[string]hostCounts),
	}
	dropped := 0
	buffered := bufio.NewReader(reader)
	for {
		line, err := buffered.ReadBytes('\n')
		complete := len(line) > 0 && line[len(line)-1] == '\n'
		trimmed := bytes.TrimSpace(line)
		switch {
		case complete && len(trimmed) > 0:
			var record Record
			if decodeErr := json.Unmarshal(trimmed, &record); decodeErr != nil {
				return history{}, 0, fmt.Errorf("parse crawl record: %w", decodeErr)
			}
			if validateErr := validateRecord(record); validateErr != nil {
				return history{}, 0, fmt.Errorf("parse crawl record: %w", validateErr)
			}
			state.apply(record)
		case !complete && len(trimmed) > 0:
			dropped++
		}
		if errors.Is(err, io.EOF) {
			return state, dropped, nil
		}
		if err != nil {
			return history{}, 0, err
		}
	}
}

// Journal appends crawl records to disk in chunks. All writes are
// serialized through one mutex, so concurrent workers can never interleave
// partial lines in the record file.
type Journal struct {
	mu         sync.Mutex
	file       *os.File
	buffer     bytes.Buffer
	pending    int
	flushEvery int
	closed     bool
}

// OpenJournal prepares the record directory and file before a crawl starts.
// It fails early when the path exists but is not a directory, when the
// directory or record file is not writable, or when existing records cannot
// be parsed. The returned history backfills resume state.
func OpenJournal(dir string, flushEvery int) (*Journal, history, error) {
	if flushEvery <= 0 {
		flushEvery = defaultFlushEvery
	}
	info, err := os.Stat(dir)
	switch {
	case err == nil && !info.IsDir():
		return nil, history{}, fmt.Errorf("record path %s is not a directory", dir)
	case err != nil && !errors.Is(err, os.ErrNotExist):
		return nil, history{}, fmt.Errorf("inspect record dir: %w", err)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, history{}, fmt.Errorf("create record dir: %w", err)
	}

	path := filepath.Join(dir, journalFileName)
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return nil, history{}, fmt.Errorf("open record file: %w", err)
	}

	state, dropped, err := loadHistory(file)
	if err != nil {
		_ = file.Close()
		return nil, history{}, fmt.Errorf("read record file %s: %w", path, err)
	}
	if dropped > 0 {
		fmt.Fprintf(
			os.Stderr,
			"gotor: discarded %d incomplete crawl record(s) from %s\n",
			dropped,
			path,
		)
	}
	return &Journal{file: file, flushEvery: flushEvery}, state, nil
}

// Append buffers one record and flushes every flushEvery records. A write
// failure is returned so the caller can mark the page accordingly instead
// of silently losing the record.
func (j *Journal) Append(record Record) error {
	line, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("encode crawl record: %w", err)
	}

	j.mu.Lock()
	defer j.mu.Unlock()
	if j.closed {
		return errors.New("crawl journal is closed")
	}
	j.buffer.Write(line)
	j.buffer.WriteByte('\n')
	j.pending++
	if j.pending >= j.flushEvery {
		return j.flushLocked()
	}
	return nil
}

// flushLocked writes the buffered chunk in one call so lines from
// concurrent workers never interleave. The buffer is kept on error so a
// later flush can retry.
func (j *Journal) flushLocked() error {
	if j.pending == 0 {
		return nil
	}
	if _, err := j.file.Write(j.buffer.Bytes()); err != nil {
		return fmt.Errorf("append crawl records: %w", err)
	}
	j.buffer.Reset()
	j.pending = 0
	if err := j.file.Sync(); err != nil {
		return fmt.Errorf("sync crawl records: %w", err)
	}
	return nil
}

// Close flushes any buffered records and closes the record file.
func (j *Journal) Close() error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.closed {
		return nil
	}
	j.closed = true
	flushErr := j.flushLocked()
	closeErr := j.file.Close()
	if flushErr != nil {
		return flushErr
	}
	return closeErr
}
