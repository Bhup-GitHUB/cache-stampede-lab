package main

import (
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestOfferedTrafficDoesNotWaitForResponses(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(150 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	previousFlags, previousArgs := flag.CommandLine, os.Args
	defer func() { flag.CommandLine, os.Args = previousFlags, previousArgs }()
	flag.CommandLine = flag.NewFlagSet("loadgen", flag.ContinueOnError)
	output := t.TempDir()
	os.Args = []string{"loadgen", "-url", server.URL, "-rate", "200", "-duration", "100ms", "-max-inflight", "1", "-metrics-urls", "", "-output", output}
	if err := run(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(output, "summary.json"))
	if err != nil {
		t.Fatal(err)
	}
	var summary struct {
		Offered    int `json:"offered"`
		Dispatched int `json:"dispatched"`
		Dropped    int `json:"dropped"`
		Completed  int `json:"completed"`
	}
	if err = json.Unmarshal(data, &summary); err != nil {
		t.Fatal(err)
	}
	if summary.Offered != 20 || summary.Dropped < 18 || summary.Dispatched > 2 || summary.Completed != summary.Dispatched {
		t.Fatalf("offered load was not independent of responses: %+v", summary)
	}
}

func TestBodyTimeoutCounted(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer server.Close()
	previousFlags, previousArgs := flag.CommandLine, os.Args
	defer func() { flag.CommandLine, os.Args = previousFlags, previousArgs }()
	flag.CommandLine = flag.NewFlagSet("loadgen", flag.ContinueOnError)
	output := t.TempDir()
	os.Args = []string{"loadgen", "-url", server.URL, "-rate", "10", "-duration", "100ms", "-timeout", "20ms", "-metrics-urls", "", "-output", output}
	if err := run(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(output, "summary.json"))
	if err != nil {
		t.Fatal(err)
	}
	var summary struct {
		Errors   int `json:"errors"`
		Timeouts int `json:"timeouts"`
	}
	if err = json.Unmarshal(data, &summary); err != nil {
		t.Fatal(err)
	}
	if summary.Errors != 1 || summary.Timeouts != 1 {
		t.Fatalf("body timeout not recorded: %+v", summary)
	}
}

func TestMetricsBaselinePrecedesTraffic(t *testing.T) {
	var reads, baselineFinished atomic.Int64
	var early atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/metrics" {
			time.Sleep(20 * time.Millisecond)
			fmt.Fprintf(w, "stampede_db_query_starts_total %d\n", reads.Load())
			baselineFinished.Add(1)
			return
		}
		if baselineFinished.Load() < 3 {
			early.Store(true)
		}
		reads.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	previousFlags, previousArgs := flag.CommandLine, os.Args
	defer func() { flag.CommandLine, os.Args = previousFlags, previousArgs }()
	flag.CommandLine = flag.NewFlagSet("loadgen", flag.ContinueOnError)
	output := t.TempDir()
	metrics := strings.Join([]string{server.URL + "/metrics", server.URL + "/metrics", server.URL + "/metrics"}, ",")
	os.Args = []string{"loadgen", "-url", server.URL, "-rate", "20", "-duration", "100ms", "-metrics-urls", metrics, "-output", output}
	if err := run(); err != nil {
		t.Fatal(err)
	}
	if early.Load() {
		t.Fatal("offered traffic before all replica baselines completed")
	}
	f, err := os.Open(filepath.Join(output, "series.csv"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	rows, err := csv.NewReader(f).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	var starts float64
	for _, row := range rows[1:] {
		if row[11] == "" {
			continue
		}
		value, err := strconv.ParseFloat(row[11], 64)
		if err != nil {
			t.Fatal(err)
		}
		starts += value
	}
	if starts != float64(reads.Load()*3) {
		t.Fatalf("initial query starts omitted: got %v, expected %d", starts, reads.Load()*3)
	}
	data, err := os.ReadFile(filepath.Join(output, "metrics-0000.prom"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), "# sample_seconds 0.000000000") || !strings.Contains(string(data), "# scrape_started_at ") {
		t.Fatal("baseline timestamps missing")
	}
}
