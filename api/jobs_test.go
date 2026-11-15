package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/DedSecInside/gotor/internal/crawl"
)

// The HTTP API and the CLI both execute crawls through crawl.Execute, so a
// job report and a direct CLI-style run must carry identical statistics.
func TestJobReportStatsMatchCLIStats(t *testing.T) {
	t.Parallel()
	target := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "text/html")
		if request.URL.Path == "/" {
			fmt.Fprint(writer, `<html><title>Root</title><a href="/child">Child</a></html>`)
			return
		}
		fmt.Fprint(writer, `<html><title>Child</title></html>`)
	}))
	defer target.Close()

	// CLI path: crawl.Execute with a record directory.
	cliRequest := crawl.Request{
		URL:               target.URL,
		Depth:             1,
		UseTor:            false,
		RequestsPerSecond: 100,
		Burst:             100,
		RecordDir:         t.TempDir(),
	}
	cliReport, err := crawl.Execute(context.Background(), cliRequest)
	if err != nil {
		t.Fatal(err)
	}

	// HTTP path: the same crawl through the job-control API.
	apiServer := httptest.NewServer(NewServer("127.0.0.1", 0).Handler())
	defer apiServer.Close()
	apiRequest := cliRequest
	apiRequest.RecordDir = t.TempDir()
	response, payload := requestJSON(t, http.MethodPost, apiServer.URL+APIBasePath+"/jobs", apiRequest)
	if response.StatusCode != http.StatusAccepted {
		t.Fatalf("start job returned %d: %s", response.StatusCode, payload)
	}
	var started Job
	if err := json.Unmarshal(payload, &started); err != nil {
		t.Fatal(err)
	}
	completed := waitForTerminalJob(t, apiServer.URL, started.ID)
	if completed.Status != JobCompleted {
		t.Fatalf("expected completed job, got %#v", completed)
	}
	if completed.Report == nil {
		t.Fatal("missing crawl report")
	}
	apiReport := *completed.Report

	if apiReport.PagesCrawled != cliReport.PagesCrawled ||
		apiReport.PagesSkipped != cliReport.PagesSkipped ||
		apiReport.HistorySuccess != cliReport.HistorySuccess ||
		apiReport.HistoryFailed != cliReport.HistoryFailed {
		t.Fatalf(
			"API stats (%d/%d/%d/%d) differ from CLI stats (%d/%d/%d/%d)",
			apiReport.PagesCrawled,
			apiReport.PagesSkipped,
			apiReport.HistorySuccess,
			apiReport.HistoryFailed,
			cliReport.PagesCrawled,
			cliReport.PagesSkipped,
			cliReport.HistorySuccess,
			cliReport.HistoryFailed,
		)
	}
	if cliReport.PagesCrawled != 2 || cliReport.HistorySuccess != 2 {
		t.Fatalf(
			"unexpected crawl stats: crawled=%d historySuccess=%d",
			cliReport.PagesCrawled,
			cliReport.HistorySuccess,
		)
	}

	// A second CLI run over the API job's record directory must observe
	// the history the API job wrote: nothing new, seed skipped once.
	replayReport, err := crawl.Execute(context.Background(), apiRequest)
	if err != nil {
		t.Fatal(err)
	}
	if replayReport.PagesCrawled != 0 || replayReport.PagesSkipped != 1 {
		t.Fatalf(
			"expected replay to skip recorded pages, got crawled=%d skipped=%d",
			replayReport.PagesCrawled,
			replayReport.PagesSkipped,
		)
	}
	if replayReport.HistorySuccess != 2 {
		t.Fatalf("expected cumulative success of 2, got %d", replayReport.HistorySuccess)
	}
}
