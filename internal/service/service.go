package service

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	randv2 "math/rand/v2"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/singleflight"
)

type Config struct {
	Mode            string
	RedisAddr       string
	DatabaseURL     string
	Instance        string
	CacheTTL        time.Duration
	LeaseTTL        time.Duration
	QueryTimeout    time.Duration
	RefreshTimeout  time.Duration
	FollowerTimeout time.Duration
	SimulatedDelay  time.Duration
	MaxDBConns      int32
	MaxConcurrentDB int
}

func DefaultConfig() Config {
	return Config{Mode: "lease", RedisAddr: "localhost:6379", DatabaseURL: "postgres://postgres:postgres@localhost:5432/stampede?sslmode=disable", Instance: "app", CacheTTL: 30 * time.Second, LeaseTTL: time.Second, QueryTimeout: 2 * time.Second, RefreshTimeout: 3 * time.Second, FollowerTimeout: 2500 * time.Millisecond, MaxDBConns: 8, MaxConcurrentDB: 8}
}

type Item struct {
	ID      int64  `json:"id"`
	Payload string `json:"payload"`
}

var ErrOverloaded = errors.New("database fallback capacity exhausted")
var ErrWaitTimeout = errors.New("refresh waiting budget exhausted")
var ErrOwnershipLost = errors.New("lease ownership lost")

type Service struct {
	cfg      Config
	db       *pgxpool.Pool
	redis    *redis.Client
	group    singleflight.Group
	ctx      context.Context
	cancel   context.CancelFunc
	slots    chan struct{}
	dbStarts prometheus.Counter
	registry *prometheus.Registry
	events   *prometheus.CounterVec
	duration *prometheus.HistogramVec
	active   *prometheus.GaugeVec
	life     sync.Mutex
	work     sync.WaitGroup
	closing  bool
}

func New(ctx context.Context, cfg Config) (*Service, error) {
	if cfg.Mode != "naive" && cfg.Mode != "singleflight" && cfg.Mode != "lease" {
		return nil, fmt.Errorf("invalid mode %q", cfg.Mode)
	}
	if cfg.CacheTTL <= 0 || cfg.LeaseTTL < time.Millisecond || cfg.QueryTimeout <= 0 || cfg.RefreshTimeout <= 0 || cfg.FollowerTimeout <= 0 || cfg.MaxDBConns < 1 || cfg.MaxConcurrentDB < 1 || cfg.SimulatedDelay < 0 {
		return nil, errors.New("invalid nonpositive limits or negative delay")
	}
	pc, err := pgxpool.ParseConfig(cfg.DatabaseURL)
	if err != nil {
		return nil, err
	}
	pc.MaxConns = cfg.MaxDBConns
	pc.ConnConfig.RuntimeParams["application_name"] = cfg.Instance
	db, err := pgxpool.NewWithConfig(ctx, pc)
	if err != nil {
		return nil, err
	}
	if err = db.Ping(ctx); err != nil {
		db.Close()
		return nil, err
	}
	base, cancel := context.WithCancel(context.Background())
	s := &Service{cfg: cfg, db: db, redis: redis.NewClient(&redis.Options{Addr: cfg.RedisAddr, MaxRetries: -1, DialTimeout: 200 * time.Millisecond, ReadTimeout: 200 * time.Millisecond, WriteTimeout: 200 * time.Millisecond, ContextTimeoutEnabled: true}), ctx: base, cancel: cancel, slots: make(chan struct{}, cfg.MaxConcurrentDB), registry: prometheus.NewRegistry()}
	labels := prometheus.Labels{"instance": cfg.Instance, "mode": cfg.Mode}
	s.events = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "stampede_events_total", Help: "Service events by kind and outcome", ConstLabels: labels}, []string{"kind", "outcome"})
	s.duration = prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "stampede_duration_seconds", Help: "Operation durations", ConstLabels: labels, Buckets: []float64{.001, .005, .01, .025, .05, .1, .2, .4, .8, 1.6, 3.2, 6.4}}, []string{"operation"})
	s.active = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "stampede_active", Help: "Active operations", ConstLabels: labels}, []string{"operation"})
	s.dbStarts = prometheus.NewCounter(prometheus.CounterOpts{Name: "stampede_db_query_starts_total", Help: "Actual PostgreSQL query starts", ConstLabels: labels})
	s.registry.MustRegister(s.events, s.duration, s.active, s.dbStarts)
	return s, nil
}

func (s *Service) Close() {
	s.life.Lock()
	s.closing = true
	s.cancel()
	s.life.Unlock()
	s.work.Wait()
	s.redis.Close()
	s.db.Close()
}
func (s *Service) begin() bool {
	s.life.Lock()
	defer s.life.Unlock()
	if s.closing {
		return false
	}
	s.work.Add(1)
	return true
}
func (s *Service) event(kind, outcome string) { s.events.WithLabelValues(kind, outcome).Inc() }
func (s *Service) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(s.registry, promhttp.HandlerOpts{}))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("GET /items/{id}", s.readHTTP)
	return mux
}
func (s *Service) readHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("X-Instance", s.cfg.Instance)
	start := time.Now()
	defer func() { s.duration.WithLabelValues("http").Observe(time.Since(start).Seconds()) }()
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id < 1 {
		http.Error(w, "invalid id", 400)
		return
	}
	item, err := s.Get(r.Context(), id)
	if err != nil {
		status := http.StatusServiceUnavailable
		if errors.Is(err, pgx.ErrNoRows) {
			status = 404
		}
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, ErrWaitTimeout) {
			status = 504
		}
		s.event("http", strconv.Itoa(status))
		http.Error(w, err.Error(), status)
		return
	}
	s.event("http", "200")
	w.Header().Set("Content-Type", "application/json")
	if err = json.NewEncoder(w).Encode(item); err != nil {
		slog.Debug("response write failed", "error", err)
	}
}
func (s *Service) cached(ctx context.Context, id int64) (Item, bool, error) {
	value, err := s.redis.Get(ctx, "data:"+strconv.FormatInt(id, 10)).Bytes()
	if errors.Is(err, redis.Nil) {
		s.event("cache", "miss")
		return Item{}, false, nil
	}
	if err != nil {
		s.event("cache", "error")
		return Item{}, false, fmt.Errorf("redis get: %w", err)
	}
	var item Item
	if err = json.Unmarshal(value, &item); err != nil {
		s.event("cache", "error")
		return Item{}, false, fmt.Errorf("invalid cached value: %w", err)
	}
	s.event("cache", "hit")
	return item, true, nil
}
func (s *Service) Get(ctx context.Context, id int64) (Item, error) {
	if !s.begin() {
		return Item{}, errors.New("service stopping")
	}
	defer s.work.Done()
	item, hit, err := s.cached(ctx, id)
	if err != nil || hit {
		return item, err
	}
	if s.cfg.Mode == "naive" {
		return s.refresh(ctx, id, "")
	}
	key := strconv.FormatInt(id, 10)
	result := s.group.DoChan(key, func() (any, error) {
		if !s.begin() {
			return Item{}, errors.New("service stopping")
		}
		defer s.work.Done()
		shared, cancel := context.WithTimeout(s.ctx, s.cfg.RefreshTimeout)
		defer cancel()
		s.event("refresh", "local_owner")
		item, hit, err := s.cached(shared, id)
		if err != nil || hit {
			return item, err
		}
		if s.cfg.Mode == "singleflight" {
			return s.refresh(shared, id, "")
		}
		return s.leased(shared, id)
	})
	s.active.WithLabelValues("waiters").Inc()
	defer s.active.WithLabelValues("waiters").Dec()
	wait, cancel := context.WithTimeout(ctx, s.cfg.FollowerTimeout)
	defer cancel()
	select {
	case <-wait.Done():
		s.event("follower", "timeout")
		return Item{}, fmt.Errorf("%w: %w", ErrWaitTimeout, wait.Err())
	case r := <-result:
		if r.Shared {
			s.event("follower", "local_shared")
		}
		if r.Err != nil {
			return Item{}, r.Err
		}
		return r.Val.(Item), nil
	}
}

var releaseScript = redis.NewScript(`if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) else return 0 end`)
var publishScript = redis.NewScript(`if redis.call('GET', KEYS[1]) == ARGV[1] then redis.call('SET', KEYS[2], ARGV[2], 'PX', ARGV[3]); return 1 else return 0 end`)

func (s *Service) leased(ctx context.Context, id int64) (Item, error) {
	key := strconv.FormatInt(id, 10)
	leaseKey := "lease:" + key
	delay := 5 * time.Millisecond
	for {
		if err := ctx.Err(); err != nil {
			return Item{}, err
		}
		item, hit, err := s.cached(ctx, id)
		if err != nil || hit {
			return item, err
		}
		tokenBytes := make([]byte, 16)
		if _, err = rand.Read(tokenBytes); err != nil {
			return Item{}, err
		}
		token := hex.EncodeToString(tokenBytes)
		reply, err := s.redis.Do(ctx, "SET", leaseKey, token, "NX", "PX", s.cfg.LeaseTTL.Milliseconds()).Text()
		if err != nil && !errors.Is(err, redis.Nil) {
			s.event("lease", "error")
			return Item{}, fmt.Errorf("redis lease: %w", err)
		}
		if err == nil && reply == "OK" {
			s.event("lease", "acquired")
			item, err = s.ownedRefresh(ctx, id, token)
			if errors.Is(err, ErrOwnershipLost) {
				s.event("lease", "expired")
				continue
			}
			return item, err
		}
		s.event("lease", "contended")
		s.event("follower", "distributed_wait")
		timer := time.NewTimer(delay/2 + time.Duration(randv2.Int64N(int64(delay/2)+1)))
		select {
		case <-ctx.Done():
			timer.Stop()
			return Item{}, ctx.Err()
		case <-timer.C:
		}
		delay = min(delay*2, 100*time.Millisecond)
	}
}
func (s *Service) ownedRefresh(ctx context.Context, id int64, token string) (Item, error) {
	defer func() {
		releaseCtx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
		defer cancel()
		n, err := releaseScript.Run(releaseCtx, s.redis, []string{"lease:" + strconv.FormatInt(id, 10)}, token).Int()
		if err != nil {
			s.event("lease_release", "error")
		} else {
			s.event("lease_release", strconv.Itoa(n))
		}
	}()
	item, hit, err := s.cached(ctx, id)
	if err != nil || hit {
		return item, err
	}
	return s.refresh(ctx, id, token)
}
func (s *Service) refresh(ctx context.Context, id int64, token string) (Item, error) {
	select {
	case s.slots <- struct{}{}:
		defer func() { <-s.slots }()
	default:
		s.event("db_fallback", "rejected")
		return Item{}, ErrOverloaded
	}
	queryCtx, cancel := context.WithTimeout(ctx, s.cfg.QueryTimeout)
	defer cancel()
	poolStart := time.Now()
	conn, err := s.db.Acquire(queryCtx)
	s.duration.WithLabelValues("pool_wait").Observe(time.Since(poolStart).Seconds())
	if err != nil {
		s.event("pool_acquire", "error")
		return Item{}, err
	}
	defer conn.Release()
	s.dbStarts.Inc()
	s.event("db_query", "started")
	s.event("refresh", "owner")
	s.active.WithLabelValues("db_query").Inc()
	defer s.active.WithLabelValues("db_query").Dec()
	start := time.Now()
	item := Item{}
	err = conn.QueryRow(queryCtx, "SELECT id, payload FROM items CROSS JOIN LATERAL (SELECT pg_sleep($2::double precision)) AS delay WHERE id = $1", id, s.cfg.SimulatedDelay.Seconds()).Scan(&item.ID, &item.Payload)
	s.duration.WithLabelValues("db_query").Observe(time.Since(start).Seconds())
	if err != nil {
		s.event("db_query", "error")
		return Item{}, err
	}
	s.event("db_query", "completed")
	encoded, err := json.Marshal(item)
	if err != nil {
		return Item{}, err
	}
	if token != "" {
		n, err := publishScript.Run(ctx, s.redis, []string{"lease:" + strconv.FormatInt(id, 10), "data:" + strconv.FormatInt(id, 10)}, token, encoded, max(s.cfg.CacheTTL.Milliseconds(), 1)).Int()
		if err != nil {
			s.event("cache", "error")
			return Item{}, fmt.Errorf("redis publish: %w", err)
		}
		if n != 1 {
			return Item{}, ErrOwnershipLost
		}
	} else {
		if err = s.redis.Set(ctx, "data:"+strconv.FormatInt(id, 10), encoded, s.cfg.CacheTTL).Err(); err != nil {
			s.event("cache", "error")
			return Item{}, fmt.Errorf("redis publish: %w", err)
		}
	}
	return item, nil
}
