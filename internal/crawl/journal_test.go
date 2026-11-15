package crawl

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func testRecord(rawURL string, status string) Record {
	return Record{
		URL:        rawURL,
		Depth:      1,
		Status:     status,
		DurationMS: 3,
		FetchedAt:  time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}
}

func readJournalLines(t *testing.T, path string) []string {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	var lines []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	return lines
}

func TestOpenJournalRejectsNonDirectoryPath(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "records.jsonl")
	if err := os.WriteFile(path, []byte{}, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, err := OpenJournal(path, 0); err == nil {
		t.Fatal("expected an error when the record path is a file")
	} else if !strings.Contains(err.Error(), "not a directory") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestOpenJournalRejectsUnwritableDirectory(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	// A directory where the record file should live makes the open fail
	// even for privileged test runners.
	if err := os.Mkdir(filepath.Join(dir, journalFileName), 0o755); err != nil {
		t.Fatal(err)
	}
	if _, _, err := OpenJournal(dir, 0); err == nil {
		t.Fatal("expected an error when the record file cannot be opened")
	}
}

func TestOpenJournalRejectsUnparsableRecords(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	content := `{"url":"http://example.com","depth":0,"status":"success","durationMs":1,"fetchedAt":"2026-01-01T00:00:00Z"}` + "\n" +
		`{"url":` + "\n"
	if err := os.WriteFile(filepath.Join(dir, journalFileName), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, err := OpenJournal(dir, 0); err == nil {
		t.Fatal("expected an error for unparsable record content")
	} else if !strings.Contains(err.Error(), "parse crawl record") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestOpenJournalDropsTruncatedLastLine(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, journalFileName)
	complete := `{"url":"http://example.com/","depth":0,"status":"success","durationMs":1,"fetchedAt":"2026-01-01T00:00:00Z"}` + "\n" +
		`{"url":"http://example.com/a","depth":1,"status":"failed","durationMs":2,"fetchedAt":"2026-01-01T00:00:01Z"}` + "\n"
	truncated := `{"url":"http://example.com/b","dep`
	if err := os.WriteFile(path, []byte(complete+truncated), 0o644); err != nil {
		t.Fatal(err)
	}

	// Capture stderr to verify the discarded count is reported.
	original := os.Stderr
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stderr = writer
	journal, history, openErr := OpenJournal(dir, 0)
	_ = writer.Close()
	os.Stderr = original
	if openErr != nil {
		t.Fatal(openErr)
	}
	defer journal.Close()

	var stderrBuilder strings.Builder
	if _, err := reader.WriteTo(&stderrBuilder); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(stderrBuilder.String(), "discarded 1 incomplete crawl record(s)") {
		t.Fatalf("expected a discard notice on stderr, got %q", stderrBuilder.String())
	}

	if len(history.succeeded) != 1 {
		t.Fatalf("expected one succeeded URL, got %#v", history.succeeded)
	}
	if _, ok := history.succeeded["http://example.com/"]; !ok {
		t.Fatalf("missing succeeded URL: %#v", history.succeeded)
	}
	if len(history.failed) != 1 || history.failed[0].URL != "http://example.com/a" {
		t.Fatalf("unexpected failed records: %#v", history.failed)
	}
	if history.successTotal != 1 || history.failedTotal != 1 {
		t.Fatalf("unexpected totals: %#v", history)
	}
	if got := history.hosts["example.com"]; got.Success != 1 || got.Failed != 1 {
		t.Fatalf("unexpected per-host counts: %#v", got)
	}
}

func TestJournalHistoryKeepsFinalStatePerURL(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	journal, _, err := OpenJournal(dir, 1)
	if err != nil {
		t.Fatal(err)
	}
	records := []Record{
		testRecord("http://example.com/flaky", RecordStatusFailed),
		testRecord("http://example.com/ok", RecordStatusSuccess),
		testRecord("http://example.com/flaky", RecordStatusSuccess),
	}
	for _, record := range records {
		if err := journal.Append(record); err != nil {
			t.Fatal(err)
		}
	}
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, history, err := OpenJournal(dir, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if _, ok := history.succeeded["http://example.com/flaky"]; !ok {
		t.Fatal("a URL whose last record succeeded must count as done")
	}
	if len(history.failed) != 0 {
		t.Fatalf("expected no pending failures, got %#v", history.failed)
	}
	if history.successTotal != 2 || history.failedTotal != 1 {
		t.Fatalf("cumulative totals must count every record: %#v", history)
	}
}

func TestJournalConcurrentAppendsDoNotInterleave(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	journal, _, err := OpenJournal(dir, 8)
	if err != nil {
		t.Fatal(err)
	}

	const workers = 16
	const recordsPerWorker = 50
	var group sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		group.Add(1)
		go func(worker int) {
			defer group.Done()
			for i := 0; i < recordsPerWorker; i++ {
				record := testRecord(
					fmt.Sprintf("http://example.com/w%d/%d", worker, i),
					RecordStatusSuccess,
				)
				if err := journal.Append(record); err != nil {
					t.Errorf("append failed: %v", err)
					return
				}
			}
		}(worker)
	}
	group.Wait()
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}

	lines := readJournalLines(t, filepath.Join(dir, journalFileName))
	if len(lines) != workers*recordsPerWorker {
		t.Fatalf("expected %d records, got %d", workers*recordsPerWorker, len(lines))
	}
	seen := make(map[string]struct{}, len(lines))
	for _, line := range lines {
		var record Record
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("interleaved or corrupt line %q: %v", line, err)
		}
		if _, ok := seen[record.URL]; ok {
			t.Fatalf("duplicate record for %s", record.URL)
		}
		seen[record.URL] = struct{}{}
	}
}

func TestJournalAppendFailureSurfaces(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	journal, _, err := OpenJournal(dir, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}
	if err := journal.Append(testRecord("http://example.com/", RecordStatusSuccess)); err == nil {
		t.Fatal("expected an error when appending to a closed journal")
	}
}
