package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"cache-stampede-lab/internal/service"
)

func main() {
	if err := run(); err != nil {
		slog.Error("server failed", "error", err)
		os.Exit(1)
	}
}
func run() error {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	cfg := service.DefaultConfig()
	cfg.Mode = env("MODE", cfg.Mode)
	cfg.RedisAddr = env("REDIS_ADDR", cfg.RedisAddr)
	cfg.DatabaseURL = env("DATABASE_URL", cfg.DatabaseURL)
	cfg.Instance = env("INSTANCE", cfg.Instance)
	for _, entry := range []struct {
		name   string
		target *time.Duration
	}{{"CACHE_TTL", &cfg.CacheTTL}, {"LEASE_TTL", &cfg.LeaseTTL}, {"QUERY_TIMEOUT", &cfg.QueryTimeout}, {"REFRESH_TIMEOUT", &cfg.RefreshTimeout}, {"FOLLOWER_TIMEOUT", &cfg.FollowerTimeout}, {"SIMULATED_DELAY", &cfg.SimulatedDelay}} {
		if value := os.Getenv(entry.name); value != "" {
			parsed, err := time.ParseDuration(value)
			if err != nil {
				return fmt.Errorf("%s: %w", entry.name, err)
			}
			*entry.target = parsed
		}
	}
	for _, entry := range []struct {
		name string
		set  func(int)
	}{{"MAX_DB_CONNS", func(n int) { cfg.MaxDBConns = int32(n) }}, {"MAX_CONCURRENT_DB", func(n int) { cfg.MaxConcurrentDB = n }}} {
		if value := os.Getenv(entry.name); value != "" {
			n, err := strconv.Atoi(value)
			if err != nil {
				return err
			}
			entry.set(n)
		}
	}
	startup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	s, err := service.New(startup, cfg)
	cancel()
	if err != nil {
		return err
	}
	defer s.Close()
	server := &http.Server{Addr: env("ADDR", ":8080"), Handler: s.Handler(), ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 30 * time.Second, WriteTimeout: cfg.RefreshTimeout + 5*time.Second}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	failures := make(chan error, 1)
	go func() { failures <- server.ListenAndServe() }()
	slog.Info("server started", "address", server.Addr, "mode", cfg.Mode, "instance", cfg.Instance, "simulated_delay", cfg.SimulatedDelay)
	select {
	case err := <-failures:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	case <-ctx.Done():
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return server.Shutdown(shutdown)
}
func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
