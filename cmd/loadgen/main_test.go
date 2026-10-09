package main

import (
	"encoding/json"
	"flag"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
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
