package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type result struct {
	scheduled, dispatched, completed time.Duration
	latency, lag                     float64
	status                           int
	err                              string
	timeout                          bool
}
type bucket struct {
	Offered, Dispatched, Dropped, Completed, Errors, Timeouts int
	Latencies, Lags                                           []float64
}
type sample struct {
	second float64
	total  float64
	valid  bool
}

func percentile(v []float64, p float64) float64 {
	if len(v) == 0 {
		return 0
	}
	sort.Float64s(v)
	return v[int(math.Ceil(float64(len(v))*p))-1]
}
func number(v float64) string { return strconv.FormatFloat(v, 'f', 3, 64) }
func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() error {
	url := flag.String("url", "http://localhost:18080/items/{key}", "Endpoint URL, optionally containing {key}")
	duration := flag.Duration("duration", 15*time.Second, "Offered traffic duration")
	rate := flag.Int("rate", 2000, "Constant offered requests per second")
	output := flag.String("output", "results/run", "Output directory")
	keys := flag.Int("keys", 1, "Number of keys, rotated deterministically from key-start")
	keyStart := flag.Int("key-start", 1, "First key")
	timeout := flag.Duration("timeout", 3*time.Second, "Individual HTTP timeout")
	maxInflight := flag.Int("max-inflight", 10000, "Maximum concurrent requests; excess offers are recorded as dropped")
	metrics := flag.String("metrics-urls", "http://localhost:18081/metrics,http://localhost:18082/metrics,http://localhost:18083/metrics", "Comma-separated replica metrics URLs; empty disables scraping")
	dbMetric := flag.String("db-metric", "stampede_db_query_starts_total", "Database query starts counter name")
	flag.Parse()
	if *rate <= 0 || *duration <= 0 || *keys <= 0 || *maxInflight <= 0 || *timeout <= 0 {
		return fmt.Errorf("rate, duration, keys, timeout and max-inflight must be positive")
	}
	if err := os.MkdirAll(*output, 0755); err != nil {
		return err
	}
	f, err := os.Create(filepath.Join(*output, "requests.csv"))
	if err != nil {
		return err
	}
	defer f.Close()
	writer := csv.NewWriter(f)
	if err = writer.Write([]string{"scheduled_seconds", "dispatched_seconds", "completed_seconds", "latency_ms", "scheduling_lag_ms", "offered_to_completion_ms", "status", "error"}); err != nil {
		return err
	}
	transport := &http.Transport{MaxIdleConns: *maxInflight, MaxIdleConnsPerHost: *maxInflight, MaxConnsPerHost: *maxInflight}
	defer transport.CloseIdleConnections()
	client := &http.Client{Timeout: *timeout, Transport: transport}
	start := time.Now()
	results := make(chan result, *maxInflight)
	slots := make(chan struct{}, *maxInflight)
	var wg sync.WaitGroup
	buckets := map[int]*bucket{}
	get := func(second int) *bucket {
		b := buckets[second]
		if b == nil {
			b = &bucket{}
			buckets[second] = b
		}
		return b
	}
	collectorDone := make(chan struct{})
	var writeErr error
	go func() {
		defer close(collectorDone)
		for r := range results {
			offered := get(int(r.scheduled.Seconds()))
			offered.Offered++
			if r.err == "inflight_limit" {
				offered.Dropped++
			} else {
				get(int(r.dispatched.Seconds())).Dispatched++
				b := get(int(r.completed.Seconds()))
				b.Completed++
				b.Latencies = append(b.Latencies, r.latency)
				b.Lags = append(b.Lags, r.lag)
				if r.err != "" || r.status >= 400 {
					b.Errors++
				}
				if r.timeout {
					b.Timeouts++
				}
			}
			if e := writer.Write([]string{number(r.scheduled.Seconds()), number(r.dispatched.Seconds()), number(r.completed.Seconds()), number(r.latency), number(r.lag), number(float64(r.completed-r.scheduled) / float64(time.Millisecond)), strconv.Itoa(r.status), r.err}); e != nil {
				writeErr = e
			}
		}
		writer.Flush()
		if e := writer.Error(); e != nil {
			writeErr = e
		}
	}()
	scrapeCtx, cancelScrape := context.WithCancel(context.Background())
	scrapeDone := make(chan []sample, 1)
	go func() { scrapeDone <- scrape(scrapeCtx, start, *metrics, *dbMetric, *output) }()
	count := int(float64(*rate) * duration.Seconds())
	for i := 0; i < count; i++ {
		scheduled := time.Duration(float64(i) * float64(time.Second) / float64(*rate))
		if wait := time.Until(start.Add(scheduled)); wait > 0 {
			time.Sleep(wait)
		}
		dispatched := time.Since(start)
		select {
		case slots <- struct{}{}:
		default:
			results <- result{scheduled: scheduled, dispatched: dispatched, completed: dispatched, err: "inflight_limit"}
			continue
		}
		key := strconv.Itoa(*keyStart + i%*keys)
		endpoint := strings.ReplaceAll(*url, "{key}", key)
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-slots }()
			begin := time.Now()
			r := result{scheduled: scheduled, dispatched: dispatched, lag: float64(dispatched-scheduled) / float64(time.Millisecond)}
			resp, e := client.Get(endpoint)
			if e != nil {
				r.err = e.Error()
				if ne, ok := e.(interface{ Timeout() bool }); ok {
					r.timeout = ne.Timeout()
				}
			} else {
				r.status = resp.StatusCode
				_, e = io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				if e != nil {
					r.err = e.Error()
					if ne, ok := e.(interface{ Timeout() bool }); ok {
						r.timeout = ne.Timeout()
					}
				}
			}
			r.completed = time.Since(start)
			r.latency = float64(time.Since(begin)) / float64(time.Millisecond)
			results <- r
		}()
	}
	if wait := time.Until(start.Add(*duration)); wait > 0 {
		time.Sleep(wait)
	}
	wg.Wait()
	elapsed := time.Since(start)
	close(results)
	<-collectorDone
	cancelScrape()
	samples := <-scrapeDone
	if writeErr != nil {
		return writeErr
	}
	sf, err := os.Create(filepath.Join(*output, "series.csv"))
	if err != nil {
		return err
	}
	defer sf.Close()
	sw := csv.NewWriter(sf)
	sw.Write([]string{"second", "offered", "dispatched", "dropped", "completed", "errors", "timeouts", "p50_ms", "p95_ms", "p99_ms", "scheduling_lag_p99_ms", "db_query_starts", "db_qps"})
	db := map[int]sample{}
	for i := 1; i < len(samples); i++ {
		a, b := samples[i-1], samples[i]
		if a.valid && b.valid && b.total >= a.total {
			previous := db[int(a.second)]
			previous.second += b.second - a.second
			previous.total += b.total - a.total
			previous.valid = true
			db[int(a.second)] = previous
		}
	}
	var allLatencies, allLags []float64
	total := bucket{}
	maxSecond := int(elapsed.Seconds())
	for second := 0; second <= maxSecond; second++ {
		b := get(second)
		total.Offered += b.Offered
		total.Dispatched += b.Dispatched
		total.Dropped += b.Dropped
		total.Completed += b.Completed
		total.Errors += b.Errors
		total.Timeouts += b.Timeouts
		allLatencies = append(allLatencies, b.Latencies...)
		allLags = append(allLags, b.Lags...)
		d := db[second]
		starts, qps := "", ""
		if d.valid {
			starts = number(d.total)
			qps = number(d.total / d.second)
		}
		sw.Write([]string{strconv.Itoa(second), strconv.Itoa(b.Offered), strconv.Itoa(b.Dispatched), strconv.Itoa(b.Dropped), strconv.Itoa(b.Completed), strconv.Itoa(b.Errors), strconv.Itoa(b.Timeouts), number(percentile(b.Latencies, .5)), number(percentile(b.Latencies, .95)), number(percentile(b.Latencies, .99)), number(percentile(b.Lags, .99)), starts, qps})
	}
	sw.Flush()
	if err = sw.Error(); err != nil {
		return err
	}
	summary := map[string]any{
		"started_at":                    start.UTC().Format(time.RFC3339Nano),
		"start_unix_seconds":            float64(start.UnixNano()) / 1e9,
		"url":                           *url,
		"configured_rate":               *rate,
		"duration_seconds":              duration.Seconds(),
		"elapsed_seconds":               elapsed.Seconds(),
		"keys":                          *keys,
		"key_start":                     *keyStart,
		"timeout":                       timeout.String(),
		"max_inflight":                  *maxInflight,
		"metrics_urls":                  *metrics,
		"offered":                       total.Offered,
		"dispatched":                    total.Dispatched,
		"dropped":                       total.Dropped,
		"completed":                     total.Completed,
		"errors":                        total.Errors,
		"timeouts":                      total.Timeouts,
		"offered_rps":                   float64(total.Offered) / duration.Seconds(),
		"completed_rps_including_drain": float64(total.Completed) / elapsed.Seconds(),
		"p50_ms":                        percentile(allLatencies, .5),
		"p95_ms":                        percentile(allLatencies, .95),
		"p99_ms":                        percentile(allLatencies, .99),
		"scheduling_lag_p99_ms":         percentile(allLags, .99),
	}
	data, err := json.MarshalIndent(summary, "", "  ")
	if err != nil {
		return err
	}
	if err = os.WriteFile(filepath.Join(*output, "summary.json"), data, 0644); err != nil {
		return err
	}
	fmt.Println(string(data))
	return nil
}
func scrape(ctx context.Context, start time.Time, urls, metric, output string) []sample {
	if urls == "" {
		return nil
	}
	client := &http.Client{Timeout: 700 * time.Millisecond}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	var samples []sample
	collect := func() {
		s := sample{second: time.Since(start).Seconds(), valid: true}
		var raw strings.Builder
		for _, url := range strings.Split(urls, ",") {
			raw.WriteString("# replica " + url + "\n")
			resp, err := client.Get(strings.TrimSpace(url))
			if err != nil {
				s.valid = false
				raw.WriteString("# scrape_error " + err.Error() + "\n")
				continue
			}
			data, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			if err != nil || resp.StatusCode != 200 {
				s.valid = false
				continue
			}
			raw.Write(data)
			found := false
			for _, line := range strings.Split(string(data), "\n") {
				fields := strings.Fields(line)
				if len(fields) < 2 {
					continue
				}
				name := strings.SplitN(fields[0], "{", 2)[0]
				if name == metric {
					value, e := strconv.ParseFloat(fields[1], 64)
					if e == nil {
						s.total += value
						found = true
					}
				}
			}
			if !found {
				s.valid = false
			}
		}
		if err := os.WriteFile(filepath.Join(output, fmt.Sprintf("metrics-%04d.prom", len(samples))), []byte(raw.String()), 0644); err != nil {
			fmt.Fprintln(os.Stderr, "write metrics:", err)
			s.valid = false
		}
		samples = append(samples, s)
	}
	collect()
	for {
		select {
		case <-ctx.Done():
			collect()
			return samples
		case <-ticker.C:
			collect()
		}
	}
}
