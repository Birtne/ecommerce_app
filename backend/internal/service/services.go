package service

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ductor/ecommerce_app/backend/internal/domain"
	"github.com/ductor/ecommerce_app/backend/internal/metrics"
	"github.com/ductor/ecommerce_app/backend/internal/repository"
	"github.com/nats-io/nats.go"
	"github.com/redis/go-redis/v9"
	"golang.org/x/crypto/bcrypt"
)

type AuthService struct{ store *repository.Store }

func NewAuthService(store *repository.Store) *AuthService { return &AuthService{store: store} }

func (s *AuthService) Register(ctx context.Context, email, password, name string) (int64, string, error) {
	h, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return 0, "", err
	}
	uid, err := s.store.CreateUser(ctx, email, string(h), name)
	if err != nil {
		return 0, "", err
	}
	t, err := generateToken()
	if err != nil {
		return 0, "", err
	}
	if err := s.store.CreateSession(ctx, uid, t); err != nil {
		return 0, "", err
	}
	return uid, t, nil
}

func (s *AuthService) Login(ctx context.Context, email, password string) (int64, string, error) {
	u, err := s.store.GetUserByEmail(ctx, email)
	if err != nil {
		return 0, "", err
	}
	if err := bcrypt.CompareHashAndPassword([]byte(u.PasswordHash), []byte(password)); err != nil {
		return 0, "", errors.New("invalid credentials")
	}
	t, err := generateToken()
	if err != nil {
		return 0, "", err
	}
	if err := s.store.CreateSession(ctx, u.ID, t); err != nil {
		return 0, "", err
	}
	return u.ID, t, nil
}

func (s *AuthService) ResolveToken(ctx context.Context, token string) (*domain.User, error) {
	return s.store.GetUserByToken(ctx, token)
}

func (s *AuthService) AdminLogin(ctx context.Context, email, password string) (int64, string, error) {
	u, err := s.store.GetUserByEmail(ctx, email)
	if err != nil {
		return 0, "", err
	}
	if u.Role != "admin" {
		return 0, "", errors.New("admin role required")
	}
	if err := bcrypt.CompareHashAndPassword([]byte(u.PasswordHash), []byte(password)); err != nil {
		return 0, "", errors.New("invalid credentials")
	}
	t, err := generateToken()
	if err != nil {
		return 0, "", err
	}
	if err := s.store.CreateAdminSession(ctx, u.ID, t, time.Now().Add(12*time.Hour)); err != nil {
		return 0, "", err
	}
	return u.ID, t, nil
}

func (s *AuthService) ResolveAdminToken(ctx context.Context, token string) (*domain.User, error) {
	u, err := s.store.GetAdminByToken(ctx, token)
	if err != nil {
		return nil, err
	}
	if u.Role != "admin" {
		return nil, errors.New("admin role required")
	}
	return u, nil
}

func (s *AuthService) AdminHasPermission(ctx context.Context, role, action string) (bool, error) {
	return s.store.RoleHasPermission(ctx, role, action)
}

func (s *AuthService) AdminLogout(ctx context.Context, token string) error {
	return s.store.RevokeAdminSession(ctx, token)
}

func (s *AuthService) CleanupExpiredAdminSessions(ctx context.Context) error {
	return s.store.CleanupExpiredAdminSessions(ctx)
}

func (s *AuthService) ListAuditLogs(ctx context.Context, action, search string, limit int32, cursorID int64) ([]repository.AuditLogRecord, error) {
	return s.store.ListAuditLogs(ctx, action, search, limit, cursorID)
}

func generateToken() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return "tok_" + hex.EncodeToString(b), nil
}

type ProductService struct {
	store *repository.Store
	rdb   *redis.Client
}

func NewProductService(store *repository.Store, rdb *redis.Client) *ProductService {
	return &ProductService{store: store, rdb: rdb}
}

func (s *ProductService) ListProducts(ctx context.Context) ([]domain.Product, error) {
	const key = "products:list"
	if s.rdb != nil {
		if cached, err := s.rdb.Get(ctx, key).Result(); err == nil {
			var items []domain.Product
			if json.Unmarshal([]byte(cached), &items) == nil {
				return items, nil
			}
		}
	}
	items, err := s.store.ListProducts(ctx)
	if err != nil {
		return nil, err
	}
	if s.rdb != nil {
		if b, err := json.Marshal(items); err == nil {
			s.rdb.Set(ctx, key, string(b), 30*time.Second)
		}
	}
	return items, nil
}

type CartService struct{ store *repository.Store }

func NewCartService(store *repository.Store) *CartService { return &CartService{store: store} }

func (s *CartService) AddOrUpdateItem(ctx context.Context, uid, pid int64, qty int32) error {
	if qty <= 0 {
		return errors.New("quantity must be > 0")
	}
	return s.store.UpsertCartItem(ctx, uid, pid, qty)
}

func (s *CartService) RemoveItem(ctx context.Context, uid, pid int64) error {
	return s.store.RemoveCartItem(ctx, uid, pid)
}

func (s *CartService) GetCart(ctx context.Context, uid int64) ([]domain.CartItem, float64, error) {
	return s.store.GetCart(ctx, uid)
}

type cachedTotal struct {
	Val int64
	Exp time.Time
}

type orderStore interface {
	CreateOrderFromCartWithIdempotency(ctx context.Context, uid int64, address, idemKey string) (*domain.Order, bool, error)
	GetOrderDetail(ctx context.Context, uid, orderID int64) (*domain.OrderDetail, error)
	ListOrders(ctx context.Context, uid int64, f repository.OrderListFilter) ([]domain.Order, int64, error)
	GetCachedOrderTotal(ctx context.Context, uid int64, status string) (int64, error)
}

type OrderService struct {
	store      orderStore
	totalCache sync.Map
}

func NewOrderService(store *repository.Store) *OrderService {
	metrics.Init()
	return &OrderService{store: store}
}

type OrderListFilter struct {
	Status       string
	Statuses     []string
	OrderIDs     []int64
	FromTime     *time.Time
	ToTime       *time.Time
	MinAmount    *float64
	MaxAmount    *float64
	Page         int32
	PageSize     int32
	CursorAt     *time.Time
	CursorID     int64
	IncludeTotal bool
}

type OrderListResult struct {
	Items      []domain.Order `json:"items"`
	Total      int64          `json:"total"`
	Page       int32          `json:"page"`
	PageSize   int32          `json:"page_size"`
	NextCursor string         `json:"next_cursor,omitempty"`
}

func (s *OrderService) PlaceOrder(ctx context.Context, uid int64, address, idemKey string) (*domain.Order, bool, error) {
	start := time.Now()
	order, replay, err := s.store.CreateOrderFromCartWithIdempotency(ctx, uid, address, idemKey)
	result := "ok"
	replayTag := "false"
	if replay {
		replayTag = "true"
	}
	if err != nil {
		result = "error"
		replayTag = "false"
	}
	metrics.OrderPlaceLatency.WithLabelValues(result).Observe(time.Since(start).Seconds())
	metrics.OrderRequestTotal.WithLabelValues(result, replayTag).Inc()
	if err == nil {
		metrics.OrderPlacedTotal.WithLabelValues(strconv.FormatBool(replay)).Inc()
	}
	return order, replay, err
}

func (s *OrderService) GetOrderDetail(ctx context.Context, uid, orderID int64) (*domain.OrderDetail, error) {
	return s.store.GetOrderDetail(ctx, uid, orderID)
}

func (s *OrderService) ListOrders(ctx context.Context, uid int64, f OrderListFilter) (*OrderListResult, error) {
	if f.Page <= 0 {
		f.Page = 1
	}
	if f.PageSize <= 0 {
		f.PageSize = 20
	}
	if f.PageSize > 100 {
		f.PageSize = 100
	}
	timeFilter := "none"
	if f.FromTime != nil || f.ToTime != nil {
		timeFilter = "range"
	}
	statusFilter := "all"
	if len(f.Statuses) > 0 {
		statusFilter = "multi"
	} else if f.Status != "" {
		statusFilter = f.Status
	}
	metrics.OrderListRequests.WithLabelValues(statusFilter, timeFilter).Inc()

	items, total, err := s.store.ListOrders(ctx, uid, repository.OrderListFilter{
		Status:       f.Status,
		Statuses:     f.Statuses,
		OrderIDs:     f.OrderIDs,
		FromTime:     f.FromTime,
		ToTime:       f.ToTime,
		MinAmount:    f.MinAmount,
		MaxAmount:    f.MaxAmount,
		Page:         f.Page,
		PageSize:     f.PageSize,
		CursorAt:     f.CursorAt,
		CursorID:     f.CursorID,
		IncludeTotal: f.IncludeTotal,
	})
	if err != nil {
		return nil, err
	}
	cursorMode := f.CursorAt != nil && f.CursorID > 0
	cacheEligible := f.FromTime == nil && f.ToTime == nil && f.MinAmount == nil && f.MaxAmount == nil && !cursorMode && len(f.OrderIDs) == 0 && len(f.Statuses) == 0
	if f.IncludeTotal && cacheEligible {
		if v, err := s.store.GetCachedOrderTotal(ctx, uid, f.Status); err == nil {
			total = v
		}
	}
	if f.IncludeTotal && cacheEligible && total >= 0 {
		key := fmt.Sprintf("%d|%s|%d", uid, f.Status, f.PageSize)
		s.totalCache.Store(key, cachedTotal{Val: total, Exp: time.Now().Add(20 * time.Second)})
	}
	if !f.IncludeTotal && cacheEligible {
		key := fmt.Sprintf("%d|%s|%d", uid, f.Status, f.PageSize)
		if val, ok := s.totalCache.Load(key); ok {
			if cv, ok := val.(cachedTotal); ok && cv.Exp.After(time.Now()) {
				total = cv.Val
			}
		}
	}
	result := &OrderListResult{
		Items:    items,
		Total:    total,
		Page:     f.Page,
		PageSize: f.PageSize,
	}
	if len(items) > 0 {
		last := items[len(items)-1]
		result.NextCursor = fmt.Sprintf("%d:%d", last.CreatedAt.UnixNano(), last.ID)
	}
	return result, nil
}

type OrderStatsService struct {
	store *repository.Store
}

func NewOrderStatsService(store *repository.Store) *OrderStatsService {
	return &OrderStatsService{store: store}
}

func (s *OrderStatsService) Start(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_, _ = s.store.RefreshOrderStats(ctx, 200)
		}
	}
}

type OutboxRuntimeMetrics struct {
	Runs         int64 `json:"runs"`
	Attempts     int64 `json:"attempts"`
	Sent         int64 `json:"sent"`
	Retried      int64 `json:"retried"`
	DeadLettered int64 `json:"dead_lettered"`
	LastRunUnix  int64 `json:"last_run_unix"`
}

type OutboxPublisher struct {
	store *repository.Store
	nc    *nats.Conn
	m     OutboxRuntimeMetrics
}

type ReplayJobService struct {
	store *repository.Store
	owner string
}

func NewReplayJobService(store *repository.Store) *ReplayJobService {
	host, _ := os.Hostname()
	return &ReplayJobService{store: store, owner: fmt.Sprintf("%s-%d", host, time.Now().UnixNano())}
}

func NewOutboxPublisher(store *repository.Store, nc *nats.Conn) *OutboxPublisher {
	metrics.Init()
	return &OutboxPublisher{store: store, nc: nc}
}

func (p *OutboxPublisher) RuntimeMetrics() OutboxRuntimeMetrics {
	return OutboxRuntimeMetrics{
		Runs:         atomic.LoadInt64(&p.m.Runs),
		Attempts:     atomic.LoadInt64(&p.m.Attempts),
		Sent:         atomic.LoadInt64(&p.m.Sent),
		Retried:      atomic.LoadInt64(&p.m.Retried),
		DeadLettered: atomic.LoadInt64(&p.m.DeadLettered),
		LastRunUnix:  atomic.LoadInt64(&p.m.LastRunUnix),
	}
}

func (p *OutboxPublisher) RunOnce(ctx context.Context) error {
	atomic.AddInt64(&p.m.Runs, 1)
	atomic.StoreInt64(&p.m.LastRunUnix, time.Now().Unix())
	if p.nc == nil {
		return nil
	}
	events, err := p.store.FetchPendingOutboxEvents(ctx, 64)
	if err != nil {
		return err
	}
	for _, evt := range events {
		atomic.AddInt64(&p.m.Attempts, 1)
		start := time.Now()
		if err := p.nc.Publish(evt.Topic, evt.Payload); err != nil {
			metrics.OutboxPublishTotal.WithLabelValues(evt.Topic, "error").Inc()
			metrics.OutboxPublishLatency.WithLabelValues(evt.Topic, "error").Observe(time.Since(start).Seconds())
			if evt.RetryCount+1 >= evt.MaxRetries {
				if dlqErr := p.store.MoveOutboxEventToDeadLetter(ctx, evt, err.Error()); dlqErr == nil {
					atomic.AddInt64(&p.m.DeadLettered, 1)
					logOutbox("error", "outbox moved to dead letter", map[string]any{
						"event_id":           evt.ID,
						"retry_count":        evt.RetryCount + 1,
						"error":              err.Error(),
						"trace_id":           evt.TraceID,
						"command_id":         evt.CommandID,
						"correlation_source": evt.CorrelationSource,
						"replay_job_id":      evt.ReplayJobID,
						"dead_letter_id":     evt.DeadLetterID,
					})
				}
			} else {
				_ = p.store.MarkOutboxEventRetry(ctx, evt.ID, err.Error())
				atomic.AddInt64(&p.m.Retried, 1)
				logOutbox("warn", "outbox publish retry", map[string]any{
					"event_id":           evt.ID,
					"retry_count":        evt.RetryCount + 1,
					"error":              err.Error(),
					"trace_id":           evt.TraceID,
					"command_id":         evt.CommandID,
					"correlation_source": evt.CorrelationSource,
					"replay_job_id":      evt.ReplayJobID,
					"dead_letter_id":     evt.DeadLetterID,
				})
			}
			continue
		}
		if err := p.store.MarkOutboxEventSent(ctx, evt.ID); err != nil {
			_ = p.store.MarkOutboxEventRetry(ctx, evt.ID, err.Error())
			atomic.AddInt64(&p.m.Retried, 1)
			metrics.OutboxPublishTotal.WithLabelValues(evt.Topic, "mark_sent_error").Inc()
			metrics.OutboxPublishLatency.WithLabelValues(evt.Topic, "mark_sent_error").Observe(time.Since(start).Seconds())
			logOutbox("warn", "outbox mark sent failed, retried", map[string]any{
				"event_id":           evt.ID,
				"error":              err.Error(),
				"trace_id":           evt.TraceID,
				"command_id":         evt.CommandID,
				"correlation_source": evt.CorrelationSource,
				"replay_job_id":      evt.ReplayJobID,
				"dead_letter_id":     evt.DeadLetterID,
			})
			continue
		}
		atomic.AddInt64(&p.m.Sent, 1)
		metrics.OutboxPublishTotal.WithLabelValues(evt.Topic, "sent").Inc()
		metrics.OutboxPublishLatency.WithLabelValues(evt.Topic, "sent").Observe(time.Since(start).Seconds())
		logOutbox("info", "outbox published", map[string]any{
			"event_id":           evt.ID,
			"topic":              evt.Topic,
			"trace_id":           evt.TraceID,
			"command_id":         evt.CommandID,
			"correlation_source": evt.CorrelationSource,
			"replay_job_id":      evt.ReplayJobID,
			"dead_letter_id":     evt.DeadLetterID,
		})
	}
	return nil
}

func (p *OutboxPublisher) ReplayDeadLetter(ctx context.Context, id int64) error {
	return p.store.ReplayDeadLetterByID(ctx, id)
}

func (p *OutboxPublisher) ReplayDeadLettersBatch(ctx context.Context, limit int32) (int, error) {
	events, err := p.store.ListDeadLetterEvents(ctx, limit)
	if err != nil {
		return 0, err
	}
	replayed := 0
	for _, evt := range events {
		if err := p.store.ReplayDeadLetterByID(ctx, evt.ID); err != nil {
			logOutbox("warn", "dead letter replay failed", map[string]any{"dead_letter_id": evt.ID, "error": err.Error()})
			continue
		}
		replayed++
	}
	return replayed, nil
}

func (p *OutboxPublisher) Start(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_ = p.RunOnce(ctx)
		}
	}
}

func (s *ReplayJobService) CreateJob(ctx context.Context, actorUserID int64, topic string, limit int32) (int64, int32, error) {
	return s.store.CreateReplayJob(ctx, actorUserID, topic, limit)
}

func (s *ReplayJobService) GetJob(ctx context.Context, jobID int64) (*repository.ReplayJob, []repository.ReplayFailedGroup, error) {
	return s.store.GetReplayJob(ctx, jobID)
}

func (s *ReplayJobService) RetryFailed(ctx context.Context, actorUserID, jobID int64, errorGroup string, limit int32, resetAttempts bool) (repository.RetryFailedReplaySummary, error) {
	_ = actorUserID
	return s.store.RetryFailedReplayItems(ctx, jobID, errorGroup, limit, resetAttempts)
}

func (s *ReplayJobService) ProcessOnce(ctx context.Context) error {
	if err := s.store.RecoverExpiredReplayLeases(ctx); err != nil {
		return err
	}
	job, err := s.store.PickQueuedReplayJob(ctx, s.owner, 30*time.Second)
	if err != nil {
		if err == repository.ErrNotFound {
			return nil
		}
		return err
	}
	logOutbox("info", "replay job picked", map[string]any{
		"job_id":      job.ID,
		"lease_owner": s.owner,
		"trace_id":    job.TraceID,
		"command_id":  job.CommandID,
	})
	jobCtx := repository.WithReplayTraceMetadata(ctx, job.ID, job.TraceID, job.CommandID)
	for {
		ids, err := s.store.FetchPendingReplayItems(jobCtx, job.ID, 32)
		if err != nil {
			return err
		}
		if len(ids) == 0 {
			break
		}
		for _, deadID := range ids {
			err := s.store.ReplayDeadLetterByID(jobCtx, deadID)
			if err != nil {
				logOutbox("warn", "replay item failed", map[string]any{
					"job_id":         job.ID,
					"dead_letter_id": deadID,
					"trace_id":       job.TraceID,
					"command_id":     job.CommandID,
					"error":          err.Error(),
				})
				_ = s.store.MarkReplayItemFailed(jobCtx, job.ID, deadID, err.Error())
				continue
			}
			_ = s.store.MarkReplayItemSuccess(jobCtx, job.ID, deadID)
		}
	}
	if err := s.store.FinalizeReplayJob(jobCtx, job.ID); err != nil {
		return err
	}
	logOutbox("info", "replay job finalized", map[string]any{
		"job_id":     job.ID,
		"trace_id":   job.TraceID,
		"command_id": job.CommandID,
	})
	return nil
}

func (s *ReplayJobService) Start(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := s.ProcessOnce(ctx); err != nil {
				logOutbox("warn", "replay job process once failed", map[string]any{"error": err.Error()})
			}
		}
	}
}

func logOutbox(level, msg string, fields map[string]any) {
	payload := map[string]any{"component": "outbox", "level": level, "msg": msg, "ts": time.Now().UTC().Format(time.RFC3339)}
	for k, v := range fields {
		payload[k] = v
	}
	if b, err := json.Marshal(payload); err == nil {
		log.Printf("%s", b)
		return
	}
	log.Printf("outbox level=%s msg=%s", level, msg)
}

func FormatErr(msg string, err error) error { return fmt.Errorf("%s: %w", msg, err) }
