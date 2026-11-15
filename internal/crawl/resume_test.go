package crawl

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
)

// resumeSite serves a root page linking to /a and /b and counts requests.
func resumeSite(t *testing.T, hits *atomic.Int32) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		hits.Add(1)
		writer.Header().Set("Content-Type", "text/html")
		switch request.URL.Path {
		case "/":
			fmt.Fprint(writer, `<html><title>Root</title><a href="/a">A</a><a href="/b">B</a></html>`)
		default:
			fmt.Fprintf(writer, `<html><title>%s</title></html>`, request.URL.Path)
		}
	}))
	t.Cleanup(server.Close)
	return server
}

func TestExecuteFirstRunAppendsRecords(t *testing.T) {
	t.Parallel()
	var hits atomic.Int32
	server := resumeSite(t, &hits)
	recordDir := t.TempDir()

	request := directRequest(server.URL, 1)
	request.RecordDir = recordDir
	report, err := Execute(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}

	if report.PagesCrawled != 3 {
		t.Fatalf("expected 3 crawled pages, got %d", report.PagesCrawled)
	}
	if report.PagesSkipped != 0 {
		t.Fatalf("expected no history skips on a first run, got %d", report.PagesSkipped)
	}
	if report.HistorySuccess != 3 || report.HistoryFailed != 0 {
		t.Fatalf(
			"unexpected history totals: success=%d failed=%d",
			report.HistorySuccess,
			report.HistoryFailed,
		)
	}

	lines := readJournalLines(t, filepath.Join(recordDir, journalFileName))
	if len(lines) != 3 {
		t.Fatalf("expected 3 journal records, got %d", len(lines))
	}
	for _, line := range lines {
		var record Record
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("unparsable record %q: %v", line, err)
		}
		if record.URL == "" || record.Status != RecordStatusSuccess || record.FetchedAt.IsZero() {
			t.Fatalf("incomplete record: %#v", record)
		}
	}
}

func TestExecuteResumeSkipsSucceededPages(t *testing.T) {
	t.Parallel()
	var hits atomic.Int32
	server := resumeSite(t, &hits)
	recordDir := t.TempDir()

	request := directRequest(server.URL, 1)
	request.RecordDir = recordDir
	first, err := Execute(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if first.PagesCrawled != 3 {
		t.Fatalf("first run crawled %d pages, expected 3", first.PagesCrawled)
	}
	firstHits := hits.Load()

	// Simulate an interrupted writer: a truncated tail must be dropped
	// without breaking the resume.
	file, err := os.OpenFile(
		filepath.Join(recordDir, journalFileName),
		os.O_APPEND|os.O_WRONLY,
		0o644,
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteString(`{"url":"http://partial`); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	second, err := Execute(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if got := hits.Load(); got != firstHits {
		t.Fatalf("resume refetched pages: hits went from %d to %d", firstHits, got)
	}
	if second.PagesCrawled != 0 {
		t.Fatalf("expected no new pages on resume, got %d", second.PagesCrawled)
	}
	if second.PagesSkipped != 1 {
		t.Fatalf("expected the seed to be skipped once, got %d", second.PagesSkipped)
	}
	if second.HistorySuccess != 3 || second.HistoryFailed != 0 {
		t.Fatalf(
			"unexpected resumed totals: success=%d failed=%d",
			second.HistorySuccess,
			second.HistoryFailed,
		)
	}
	if len(second.Pages) != 0 {
		t.Fatalf("expected no new page results on resume, got %d", len(second.Pages))
	}
}

func TestExecuteResumeRequeuesFailedPages(t *testing.T) {
	t.Parallel()
	var failFlaky atomic.Bool
	failFlaky.Store(true)
	var flakyHits atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "text/html")
		switch request.URL.Path {
		case "/":
			fmt.Fprint(writer, `<html><title>Root</title><a href="/flaky">Flaky</a></html>`)
		case "/flaky":
			flakyHits.Add(1)
			if failFlaky.Load() {
				hijacker, ok := writer.(http.Hijacker)
				if !ok {
					t.Error("test server does not support hijacking")
					return
				}
				conn, _, err := hijacker.Hijack()
				if err == nil {
					_ = conn.Close()
				}
				return
			}
			fmt.Fprint(writer, `<html><title>Flaky</title></html>`)
		}
	}))
	defer server.Close()
	recordDir := t.TempDir()

	request := directRequest(server.URL, 1)
	request.RecordDir = recordDir
	first, err := Execute(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if first.HistorySuccess != 1 || first.HistoryFailed != 1 {
		t.Fatalf(
			"unexpected first-run totals: success=%d failed=%d",
			first.HistorySuccess,
			first.HistoryFailed,
		)
	}
	if len(first.Errors) != 1 {
		t.Fatalf("expected one crawl error, got %#v", first.Errors)
	}
	firstFlakyHits := flakyHits.Load()

	failFlaky.Store(false)
	second, err := Execute(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if got := flakyHits.Load(); got != firstFlakyHits+1 {
		t.Fatalf("expected the failed page to be retried once, got %d extra fetches", got-firstFlakyHits)
	}
	if second.PagesCrawled != 1 {
		t.Fatalf("expected only the retried page to be crawled, got %d", second.PagesCrawled)
	}
	if second.PagesSkipped != 1 {
		t.Fatalf("expected the succeeded root to be skipped, got %d skips", second.PagesSkipped)
	}
	if second.HistorySuccess != 2 || second.HistoryFailed != 1 {
		t.Fatalf(
			"unexpected cumulative totals: success=%d failed=%d",
			second.HistorySuccess,
			second.HistoryFailed,
		)
	}
	if len(second.Errors) != 0 {
		t.Fatalf("unexpected errors on retry: %#v", second.Errors)
	}
}

func TestExecuteFailsBeforeCrawlWhenRecordDirInvalid(t *testing.T) {
	t.Parallel()
	var hits atomic.Int32
	server := resumeSite(t, &hits)

	// The record path exists but is a regular file.
	filePath := filepath.Join(t.TempDir(), "records.jsonl")
	if err := os.WriteFile(filePath, []byte{}, 0o644); err != nil {
		t.Fatal(err)
	}
	request := directRequest(server.URL, 1)
	request.RecordDir = filePath
	if _, err := Execute(context.Background(), request); err == nil {
		t.Fatal("expected an error when the record path is not a directory")
	}
	if got := hits.Load(); got != 0 {
		t.Fatalf("crawl fetched %d pages despite the record dir error", got)
	}

	// Existing records cannot be parsed.
	recordDir := t.TempDir()
	if err := os.WriteFile(
		filepath.Join(recordDir, journalFileName),
		[]byte("not json\n"),
		0o644,
	); err != nil {
		t.Fatal(err)
	}
	request.RecordDir = recordDir
	if _, err := Execute(context.Background(), request); err == nil {
		t.Fatal("expected an error for unparsable records")
	}
	if got := hits.Load(); got != 0 {
		t.Fatalf("crawl fetched %d pages despite the record parse error", got)
	}
}
