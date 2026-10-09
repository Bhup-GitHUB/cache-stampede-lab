package service

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"
)

func integrationService(t *testing.T, mode string, change func(*Config)) *Service {
	t.Helper()
	database, redisAddr := os.Getenv("TEST_DATABASE_URL"), os.Getenv("TEST_REDIS_ADDR")
	if database == "" || redisAddr == "" {
		t.Skip("real Redis/PostgreSQL integration requires TEST_DATABASE_URL and TEST_REDIS_ADDR")
	}
	cfg := DefaultConfig()
	cfg.Mode, cfg.DatabaseURL, cfg.RedisAddr = mode, database, redisAddr
	cfg.SimulatedDelay = 150 * time.Millisecond
	cfg.LeaseTTL = time.Second
	cfg.RefreshTimeout = 2 * time.Second
	cfg.FollowerTimeout = time.Second
	if change != nil {
		change(&cfg)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s, err := New(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	return s
}

func cleanKey(t *testing.T, s *Service, id int64) {
	t.Helper()
	keys := []string{fmt.Sprintf("data:%d", id), fmt.Sprintf("lease:%d", id)}
	if err := s.redis.Del(context.Background(), keys...).Err(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.redis.Del(context.Background(), keys...) })
}

func eventCount(t *testing.T, s *Service, kind, outcome string) float64 {
	t.Helper()
	families, err := s.registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() != "stampede_events_total" {
			continue
		}
		for _, metric := range family.Metric {
			labels := map[string]string{}
			for _, label := range metric.Label {
				labels[label.GetName()] = label.GetValue()
			}
			if labels["kind"] == kind && labels["outcome"] == outcome {
				return metric.GetCounter().GetValue()
			}
		}
	}
	return 0
}

func await(t *testing.T, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if check() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition did not become true before deadline")
}

func concurrentReads(t *testing.T, services []*Service, id int64) {
	t.Helper()
	start := make(chan struct{})
	failures := make(chan error, 32)
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			item, err := services[i%len(services)].Get(context.Background(), id)
			if err != nil {
				failures <- err
			} else if item.ID != id {
				failures <- fmt.Errorf("got id %d, want %d", item.ID, id)
			}
		}(i)
	}
	close(start)
	wg.Wait()
	close(failures)
	for err := range failures {
		t.Error(err)
	}
}

func TestLocalDuplicateSuppression(t *testing.T) {
	s := integrationService(t, "singleflight", nil)
	cleanKey(t, s, 9001)
	concurrentReads(t, []*Service{s}, 9001)
	if n := eventCount(t, s, "db_query", "started"); n != 1 {
		t.Fatalf("database queries = %v, want 1", n)
	}
}

func TestSeparateInstancesCoordinate(t *testing.T) {
	first := integrationService(t, "lease", nil)
	second := integrationService(t, "lease", nil)
	cleanKey(t, first, 9002)
	concurrentReads(t, []*Service{first, second}, 9002)
	if n := eventCount(t, first, "db_query", "started") + eventCount(t, second, "db_query", "started"); n != 1 {
		t.Fatalf("cross-instance database queries = %v, want 1", n)
	}
}

func TestOwnershipSafeReleaseAndPublication(t *testing.T) {
	s := integrationService(t, "lease", nil)
	cleanKey(t, s, 9003)
	ctx := context.Background()
	lease, data := "lease:9003", "data:9003"
	if err := s.redis.Set(ctx, lease, "new-owner", time.Second).Err(); err != nil {
		t.Fatal(err)
	}
	n, err := releaseScript.Run(ctx, s.redis, []string{lease}, "old-owner").Int()
	if err != nil || n != 0 {
		t.Fatalf("stale release = %d, %v", n, err)
	}
	if token, err := s.redis.Get(ctx, lease).Result(); err != nil || token != "new-owner" {
		t.Fatalf("new owner's lease lost: %q %v", token, err)
	}
	n, err = publishScript.Run(ctx, s.redis, []string{lease, data}, "new-owner", "new", 1000).Int()
	if err != nil || n != 1 {
		t.Fatalf("current publish = %d, %v", n, err)
	}
	n, err = publishScript.Run(ctx, s.redis, []string{lease, data}, "old-owner", "stale", 1000).Int()
	if err != nil || n != 0 {
		t.Fatalf("stale publish = %d, %v", n, err)
	}
	if value, err := s.redis.Get(ctx, data).Result(); err != nil || value != "new" {
		t.Fatalf("new data overwritten: %q %v", value, err)
	}
	n, err = releaseScript.Run(ctx, s.redis, []string{lease}, "new-owner").Int()
	if err != nil || n != 1 {
		t.Fatalf("current release = %d, %v", n, err)
	}
}

func TestExpiredOwnerCannotPublish(t *testing.T) {
	s := integrationService(t, "lease", nil)
	cleanKey(t, s, 9004)
	ctx := context.Background()
	if err := s.redis.Set(ctx, "lease:9004", "expired", 30*time.Millisecond).Err(); err != nil {
		t.Fatal(err)
	}
	await(t, func() bool { return s.redis.Exists(ctx, "lease:9004").Val() == 0 })
	n, err := publishScript.Run(ctx, s.redis, []string{"lease:9004", "data:9004"}, "expired", "stale", 1000).Int()
	if err != nil || n != 0 {
		t.Fatalf("expired publish = %d, %v", n, err)
	}
	if n := s.redis.Exists(ctx, "data:9004").Val(); n != 0 {
		t.Fatal("expired owner published data")
	}
}

func TestFollowerTimeoutDoesNotQueryDatabase(t *testing.T) {
	s := integrationService(t, "lease", func(c *Config) { c.FollowerTimeout = 60 * time.Millisecond; c.RefreshTimeout = 100 * time.Millisecond })
	cleanKey(t, s, 9005)
	if err := s.redis.Set(context.Background(), "lease:9005", "other-owner", time.Second).Err(); err != nil {
		t.Fatal(err)
	}
	_, err := s.Get(context.Background(), 9005)
	if !errors.Is(err, ErrWaitTimeout) {
		t.Fatalf("got %v, want follower timeout", err)
	}
	time.Sleep(150 * time.Millisecond)
	if n := eventCount(t, s, "db_query", "started"); n != 0 {
		t.Fatalf("timed-out follower started %v queries", n)
	}
}

func TestFollowerRetriesAfterOwnerDisappears(t *testing.T) {
	s := integrationService(t, "lease", nil)
	cleanKey(t, s, 9006)
	if err := s.redis.Set(context.Background(), "lease:9006", "vanished-owner", 70*time.Millisecond).Err(); err != nil {
		t.Fatal(err)
	}
	item, err := s.Get(context.Background(), 9006)
	if err != nil || item.ID != 9006 {
		t.Fatalf("owner recovery: %+v %v", item, err)
	}
	if n := eventCount(t, s, "db_query", "started"); n != 1 {
		t.Fatalf("recovery queries = %v, want 1", n)
	}
}

func TestRedisFailureNeverBecomesMiss(t *testing.T) {
	for _, mode := range []string{"naive", "singleflight", "lease"} {
		t.Run(mode, func(t *testing.T) {
			s := integrationService(t, mode, nil)
			if err := s.redis.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := s.Get(context.Background(), 9007); err == nil {
				t.Fatal("expected Redis failure")
			}
			if n := eventCount(t, s, "db_query", "started"); n != 0 {
				t.Fatalf("Redis failure caused %v database queries", n)
			}
			if n := eventCount(t, s, "cache", "error"); n != 1 {
				t.Fatalf("cache errors = %v, want 1", n)
			}
			if n := eventCount(t, s, "cache", "miss"); n != 0 {
				t.Fatalf("Redis failure counted as %v misses", n)
			}
		})
	}
}

func TestCallerCancellationDoesNotCancelSharedRefresh(t *testing.T) {
	for _, mode := range []string{"singleflight", "lease"} {
		t.Run(mode, func(t *testing.T) {
			s := integrationService(t, mode, func(c *Config) { c.SimulatedDelay = 250 * time.Millisecond })
			cleanKey(t, s, 9008)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			first := make(chan error, 1)
			go func() { _, err := s.Get(ctx, 9008); first <- err }()
			await(t, func() bool { return eventCount(t, s, "db_query", "started") == 1 })
			cancel()
			select {
			case err := <-first:
				if err == nil {
					t.Fatal("cancelled caller succeeded")
				}
			case <-time.After(time.Second):
				t.Fatal("cancelled caller blocked")
			}
			item, err := s.Get(context.Background(), 9008)
			if err != nil || item.ID != 9008 {
				t.Fatalf("surviving caller: %+v %v", item, err)
			}
			if n := eventCount(t, s, "db_query", "started"); n != 1 {
				t.Fatalf("cancellation caused %v queries", n)
			}
		})
	}
}

func TestLeaseExpiryDuringDatabaseQuery(t *testing.T) {
	slow := integrationService(t, "lease", func(c *Config) { c.LeaseTTL = 70 * time.Millisecond; c.SimulatedDelay = 250 * time.Millisecond })
	fast := integrationService(t, "lease", func(c *Config) { c.SimulatedDelay = 0 })
	cleanKey(t, slow, 9009)
	result := make(chan error, 1)
	go func() { _, err := slow.Get(context.Background(), 9009); result <- err }()
	await(t, func() bool { return eventCount(t, slow, "db_query", "started") == 1 })
	await(t, func() bool { return slow.redis.Exists(context.Background(), "lease:9009").Val() == 0 })
	if _, err := fast.Get(context.Background(), 9009); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("slow owner did not recover")
	}
	if n := eventCount(t, slow, "lease", "expired"); n != 1 {
		t.Fatalf("expired owners = %v, want 1", n)
	}
	if n := eventCount(t, slow, "db_query", "started") + eventCount(t, fast, "db_query", "started"); n != 2 {
		t.Fatalf("overlapping queries = %v, want 2", n)
	}
}

func TestLocalCoordinationDoesNotCrossInstances(t *testing.T) {
	first := integrationService(t, "singleflight", func(c *Config) { c.SimulatedDelay = 300 * time.Millisecond })
	second := integrationService(t, "singleflight", func(c *Config) { c.SimulatedDelay = 300 * time.Millisecond })
	cleanKey(t, first, 9010)
	result := make(chan error, 1)
	go func() { _, err := first.Get(context.Background(), 9010); result <- err }()
	await(t, func() bool { return eventCount(t, first, "db_query", "started") == 1 })
	if _, err := second.Get(context.Background(), 9010); err != nil {
		t.Fatal(err)
	}
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	if n := eventCount(t, first, "db_query", "started") + eventCount(t, second, "db_query", "started"); n != 2 {
		t.Fatalf("local-only queries = %v, want 2", n)
	}
}

func TestDatabaseFallbackLimitRejectsWithoutQuery(t *testing.T) {
	s := integrationService(t, "naive", func(c *Config) { c.MaxConcurrentDB = 1; c.SimulatedDelay = 300 * time.Millisecond })
	cleanKey(t, s, 9011)
	cleanKey(t, s, 9012)
	result := make(chan error, 1)
	go func() { _, err := s.Get(context.Background(), 9011); result <- err }()
	await(t, func() bool { return eventCount(t, s, "db_query", "started") == 1 })
	if _, err := s.Get(context.Background(), 9012); !errors.Is(err, ErrOverloaded) {
		t.Fatalf("got %v, want overload rejection", err)
	}
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	if n := eventCount(t, s, "db_query", "started"); n != 1 {
		t.Fatalf("limited queries = %v, want 1", n)
	}
	if n := eventCount(t, s, "db_fallback", "rejected"); n != 1 {
		t.Fatalf("rejections = %v, want 1", n)
	}
}
