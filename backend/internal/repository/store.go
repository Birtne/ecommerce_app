package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/ductor/ecommerce_app/backend/internal/domain"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

var ErrNotFound = errors.New("not found")
var ErrEmptyCart = errors.New("empty cart")
var ErrIdempotencyInProgress = errors.New("idempotency key is being processed")

const adminCommandProcessingJSON = `{"_state":"processing"}`

func isAdminCommandProcessingPayload(body []byte) bool {
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		return false
	}
	state, _ := payload["_state"].(string)
	return state == "processing"
}

type Store struct{ DB *pgxpool.Pool }

type OutboxEvent struct {
	ID                int64
	Topic             string
	Payload           []byte
	RetryCount        int32
	MaxRetries        int32
	TraceID           string
	CommandID         string
	CorrelationSource string
	ReplayJobID       int64
	DeadLetterID      int64
}

type OutboxEventRecord struct {
	ID                int64     `json:"id"`
	Topic             string    `json:"topic"`
	Status            string    `json:"status"`
	RetryCount        int32     `json:"retry_count"`
	MaxRetries        int32     `json:"max_retries"`
	TraceID           string    `json:"trace_id"`
	CommandID         string    `json:"command_id"`
	CorrelationSource string    `json:"correlation_source"`
	ReplayJobID       int64     `json:"replay_job_id"`
	DeadLetterID      int64     `json:"dead_letter_id"`
	CreatedAt         time.Time `json:"created_at"`
	UpdatedAt         time.Time `json:"updated_at"`
}

type OutboxStats struct {
	Pending      int64 `json:"pending"`
	Sent         int64 `json:"sent"`
	Failed       int64 `json:"failed"`
	DeadLetter   int64 `json:"dead_letter"`
	TotalDLQ     int64 `json:"total_dead_letter_events"`
	TotalRetries int64 `json:"total_retries"`
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

type DeadLetterEvent struct {
	ID         int64     `json:"id"`
	Topic      string    `json:"topic"`
	Payload    []byte    `json:"payload"`
	LastError  string    `json:"last_error"`
	RetryCount int32     `json:"retry_count"`
	MovedAt    time.Time `json:"moved_at"`
}

type ReplayJob struct {
	ID             int64     `json:"job_id"`
	Status         string    `json:"status"`
	TopicFilter    string    `json:"topic_filter"`
	TraceID        string    `json:"trace_id"`
	CommandID      string    `json:"command_id"`
	TotalItems     int32     `json:"total_items"`
	ProcessedItems int32     `json:"processed_items"`
	SuccessItems   int32     `json:"success_items"`
	FailedItems    int32     `json:"failed_items"`
	LastError      string    `json:"last_error"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}

type ReplayFailedGroup struct {
	ErrorGroup string `json:"error_group"`
	Count      int32  `json:"count"`
}

type RetryFailedReplaySummary struct {
	Retried             int32            `json:"retried"`
	AttemptsBeforeTotal int64            `json:"attempts_before_total"`
	AttemptsAfterTotal  int64            `json:"attempts_after_total"`
	ErrorGroupsBefore   map[string]int32 `json:"error_groups_before"`
	ErrorGroupsAfter    map[string]int32 `json:"error_groups_after"`
}

type AuditLogRecord struct {
	ID         int64     `json:"id"`
	ActorUser  int64     `json:"actor_user_id"`
	Action     string    `json:"action"`
	TargetType string    `json:"target_type"`
	TargetID   string    `json:"target_id"`
	Payload    []byte    `json:"payload"`
	CreatedAt  time.Time `json:"created_at"`
}

type traceCtxKey string

const (
	traceIDCtxKey   traceCtxKey = "trace_id"
	commandIDCtxKey traceCtxKey = "command_id"
	replayJobCtxKey traceCtxKey = "replay_job_id"
)

func WithTraceMetadata(ctx context.Context, traceID, commandID string) context.Context {
	ctx = context.WithValue(ctx, traceIDCtxKey, strings.TrimSpace(traceID))
	ctx = context.WithValue(ctx, commandIDCtxKey, strings.TrimSpace(commandID))
	return ctx
}

func WithReplayTraceMetadata(ctx context.Context, replayJobID int64, traceID, commandID string) context.Context {
	ctx = WithTraceMetadata(ctx, traceID, commandID)
	return context.WithValue(ctx, replayJobCtxKey, replayJobID)
}

func traceMetadataFromContext(ctx context.Context) (traceID, commandID string, replayJobID int64) {
	if v, ok := ctx.Value(traceIDCtxKey).(string); ok {
		traceID = strings.TrimSpace(v)
	}
	if v, ok := ctx.Value(commandIDCtxKey).(string); ok {
		commandID = strings.TrimSpace(v)
	}
	if v, ok := ctx.Value(replayJobCtxKey).(int64); ok {
		replayJobID = v
	}
	return traceID, commandID, replayJobID
}

func NewStore(db *pgxpool.Pool) *Store { return &Store{DB: db} }

func (s *Store) CreateUser(ctx context.Context, email, hash, name string) (int64, error) {
	var id int64
	err := s.DB.QueryRow(ctx, `INSERT INTO users(email,password_hash,name) VALUES($1,$2,$3) RETURNING id`, email, hash, name).Scan(&id)
	return id, err
}

func (s *Store) GetUserByEmail(ctx context.Context, email string) (*domain.User, error) {
	u := &domain.User{}
	err := s.DB.QueryRow(ctx, `SELECT id,email,password_hash,name,role FROM users WHERE email=$1`, email).Scan(&u.ID, &u.Email, &u.PasswordHash, &u.Name, &u.Role)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return u, err
}

func (s *Store) CreateSession(ctx context.Context, uid int64, token string) error {
	_, err := s.DB.Exec(ctx, `INSERT INTO sessions(user_id,token) VALUES($1,$2)`, uid, token)
	return err
}

func (s *Store) GetUserByToken(ctx context.Context, token string) (*domain.User, error) {
	u := &domain.User{}
	err := s.DB.QueryRow(ctx, `SELECT u.id,u.email,u.password_hash,u.name,u.role FROM users u JOIN sessions s ON u.id=s.user_id WHERE s.token=$1`, token).Scan(&u.ID, &u.Email, &u.PasswordHash, &u.Name, &u.Role)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return u, err
}

func (s *Store) CreateAdminSession(ctx context.Context, uid int64, token string, expiresAt time.Time) error {
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, `SELECT token, expires_at FROM admin_sessions WHERE user_id=$1 AND revoked_at IS NULL AND expires_at > NOW()`, uid)
	if err != nil {
		return err
	}
	type oldSession struct {
		Token     string
		ExpiresAt time.Time
	}
	old := make([]oldSession, 0, 4)
	for rows.Next() {
		var s oldSession
		if err := rows.Scan(&s.Token, &s.ExpiresAt); err != nil {
			rows.Close()
			return err
		}
		old = append(old, s)
	}
	rows.Close()
	for _, s0 := range old {
		if _, err := tx.Exec(ctx, `UPDATE admin_sessions SET revoked_at=NOW() WHERE token=$1 AND revoked_at IS NULL`, s0.Token); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO admin_session_blacklist(token, reason, expires_at) VALUES($1,$2,$3) ON CONFLICT(token) DO NOTHING`, s0.Token, "forced_rotation", s0.ExpiresAt); err != nil {
			return err
		}
	}
	if _, err := tx.Exec(ctx, `INSERT INTO admin_sessions(user_id, token, expires_at, last_seen_at) VALUES($1,$2,$3,NOW())`, uid, token, expiresAt); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Store) GetAdminByToken(ctx context.Context, token string) (*domain.User, error) {
	u := &domain.User{}
	err := s.DB.QueryRow(ctx, `
SELECT u.id,u.email,u.password_hash,u.name,u.role
FROM users u
JOIN admin_sessions s ON s.user_id=u.id
WHERE s.token=$1
  AND s.expires_at > NOW()
  AND s.revoked_at IS NULL
  AND s.last_seen_at > NOW() - INTERVAL '30 minutes'
  AND NOT EXISTS (SELECT 1 FROM admin_session_blacklist b WHERE b.token = s.token)`, token).Scan(&u.ID, &u.Email, &u.PasswordHash, &u.Name, &u.Role)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err == nil {
		_, _ = s.DB.Exec(ctx, `UPDATE admin_sessions SET last_seen_at=NOW() WHERE token=$1`, token)
	}
	return u, err
}

func (s *Store) RevokeAdminSession(ctx context.Context, token string) error {
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var expiresAt time.Time
	if err := tx.QueryRow(ctx, `SELECT expires_at FROM admin_sessions WHERE token=$1`, token).Scan(&expiresAt); err == nil {
		if _, err := tx.Exec(ctx, `INSERT INTO admin_session_blacklist(token, reason, expires_at) VALUES($1,$2,$3) ON CONFLICT(token) DO NOTHING`, token, "manual_logout", expiresAt); err != nil {
			return err
		}
	}
	if _, err := tx.Exec(ctx, `UPDATE admin_sessions SET revoked_at=NOW() WHERE token=$1 AND revoked_at IS NULL`, token); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Store) CleanupExpiredAdminSessions(ctx context.Context) error {
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	rows, err := tx.Query(ctx, `
SELECT token, expires_at
FROM admin_sessions
WHERE revoked_at IS NULL
  AND (expires_at <= NOW() OR last_seen_at <= NOW() - INTERVAL '30 minutes')`)
	if err != nil {
		return err
	}
	type expired struct {
		Token     string
		ExpiresAt time.Time
	}
	items := make([]expired, 0, 32)
	for rows.Next() {
		var it expired
		if err := rows.Scan(&it.Token, &it.ExpiresAt); err != nil {
			rows.Close()
			return err
		}
		items = append(items, it)
	}
	rows.Close()
	for _, it := range items {
		if _, err := tx.Exec(ctx, `INSERT INTO admin_session_blacklist(token, reason, expires_at) VALUES($1,$2,$3) ON CONFLICT(token) DO NOTHING`, it.Token, "expired_or_idle", it.ExpiresAt); err != nil {
			return err
		}
	}
	_, err = tx.Exec(ctx, `
UPDATE admin_sessions
SET revoked_at = NOW()
WHERE revoked_at IS NULL
  AND (expires_at <= NOW() OR last_seen_at <= NOW() - INTERVAL '30 minutes')`)
	if err != nil {
		return err
	}
	_, _ = tx.Exec(ctx, `DELETE FROM admin_session_blacklist WHERE expires_at IS NOT NULL AND expires_at < NOW() - INTERVAL '1 day'`)
	return tx.Commit(ctx)
}

func (s *Store) RoleHasPermission(ctx context.Context, role, action string) (bool, error) {
	var exists bool
	if err := s.DB.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM role_permissions WHERE role=$1 AND action=$2)`, role, action).Scan(&exists); err != nil {
		return false, err
	}
	return exists, nil
}

func (s *Store) GetAdminCommandResult(ctx context.Context, actorUserID int64, action, commandID string) ([]byte, bool, error) {
	var body []byte
	err := s.DB.QueryRow(ctx, `
SELECT response_json
FROM admin_command_idempotency
WHERE actor_user_id=$1 AND action=$2 AND command_id=$3`, actorUserID, action, commandID).Scan(&body)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	if isAdminCommandProcessingPayload(body) {
		return nil, false, nil
	}
	return body, true, nil
}

func (s *Store) TryBeginAdminCommand(ctx context.Context, actorUserID int64, action, commandID string) (bool, error) {
	tag, err := s.DB.Exec(ctx, `
INSERT INTO admin_command_idempotency(actor_user_id, action, command_id, response_json, created_at, updated_at)
VALUES($1,$2,$3,$4::jsonb,NOW(),NOW())
ON CONFLICT(actor_user_id, action, command_id) DO NOTHING`, actorUserID, action, commandID, adminCommandProcessingJSON)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

func (s *Store) SaveAdminCommandResult(ctx context.Context, actorUserID int64, action, commandID string, responseJSON []byte) error {
	_, err := s.DB.Exec(ctx, `
INSERT INTO admin_command_idempotency(actor_user_id, action, command_id, response_json, created_at, updated_at)
VALUES($1,$2,$3,$4,NOW(),NOW())
ON CONFLICT(actor_user_id, action, command_id)
DO UPDATE SET response_json=EXCLUDED.response_json, updated_at=NOW()`, actorUserID, action, commandID, responseJSON)
	return err
}

func (s *Store) InsertAuditLog(ctx context.Context, actorUserID int64, action, targetType, targetID string, payload any) error {
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	_, err = s.DB.Exec(ctx, `
INSERT INTO audit_logs(actor_user_id, action, target_type, target_id, payload)
VALUES($1,$2,$3,$4,$5)`, actorUserID, action, targetType, targetID, raw)
	return err
}

func (s *Store) ListAuditLogs(ctx context.Context, action, search string, limit int32, cursorID int64) ([]AuditLogRecord, error) {
	if limit <= 0 {
		limit = 50
	}
	if limit > 200 {
		limit = 200
	}
	query := `
SELECT id, COALESCE(actor_user_id,0), action, target_type, COALESCE(target_id,''), payload::text, created_at
FROM audit_logs
WHERE 1=1`
	args := make([]any, 0, 3)
	argPos := 1
	if action != "" {
		query += fmt.Sprintf(" AND action = $%d", argPos)
		args = append(args, action)
		argPos++
	}
	if search != "" {
		query += fmt.Sprintf(" AND to_tsvector('simple', coalesce(action,'') || ' ' || coalesce(target_type,'') || ' ' || coalesce(target_id,'')) @@ plainto_tsquery('simple', $%d)", argPos)
		args = append(args, search)
		argPos++
	}
	if cursorID > 0 {
		query += fmt.Sprintf(" AND id < $%d", argPos)
		args = append(args, cursorID)
		argPos++
	}
	query += fmt.Sprintf(" ORDER BY id DESC LIMIT $%d", argPos)
	args = append(args, limit)

	rows, err := s.DB.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]AuditLogRecord, 0, limit)
	for rows.Next() {
		var it AuditLogRecord
		if err := rows.Scan(&it.ID, &it.ActorUser, &it.Action, &it.TargetType, &it.TargetID, &it.Payload, &it.CreatedAt); err != nil {
			return nil, err
		}
		items = append(items, it)
	}
	return items, nil
}

func (s *Store) ListProducts(ctx context.Context) ([]domain.Product, error) {
	rows, err := s.DB.Query(ctx, `SELECT id,title,price,stock FROM products ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]domain.Product, 0)
	for rows.Next() {
		var p domain.Product
		if err := rows.Scan(&p.ID, &p.Title, &p.Price, &p.Stock); err != nil {
			return nil, err
		}
		items = append(items, p)
	}
	return items, nil
}

func (s *Store) UpsertCartItem(ctx context.Context, uid, pid int64, qty int32) error {
	_, err := s.DB.Exec(ctx, `INSERT INTO cart_items(user_id,product_id,quantity) VALUES($1,$2,$3)
	ON CONFLICT(user_id,product_id) DO UPDATE SET quantity=EXCLUDED.quantity`, uid, pid, qty)
	return err
}

func (s *Store) RemoveCartItem(ctx context.Context, uid, pid int64) error {
	_, err := s.DB.Exec(ctx, `DELETE FROM cart_items WHERE user_id=$1 AND product_id=$2`, uid, pid)
	return err
}

func (s *Store) GetCart(ctx context.Context, uid int64) ([]domain.CartItem, float64, error) {
	rows, err := s.DB.Query(ctx, `SELECT ci.product_id,p.title,p.price,ci.quantity FROM cart_items ci JOIN products p ON p.id=ci.product_id WHERE ci.user_id=$1`, uid)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	items := make([]domain.CartItem, 0)
	total := 0.0
	for rows.Next() {
		var it domain.CartItem
		if err := rows.Scan(&it.ProductID, &it.Title, &it.Price, &it.Quantity); err != nil {
			return nil, 0, err
		}
		total += it.Price * float64(it.Quantity)
		items = append(items, it)
	}
	return items, total, nil
}

func (s *Store) ListOrders(ctx context.Context, uid int64, f OrderListFilter) ([]domain.Order, int64, error) {
	f, offset := normalizeOrderListFilter(f)

	where := []string{"user_id=$1"}
	args := []any{uid}
	argPos := 2
	if len(f.Statuses) > 0 {
		where = append(where, fmt.Sprintf("status = ANY($%d)", argPos))
		args = append(args, f.Statuses)
		argPos++
	} else if f.Status != "" {
		where = append(where, fmt.Sprintf("status=$%d", argPos))
		args = append(args, f.Status)
		argPos++
	}
	if len(f.OrderIDs) > 0 {
		where = append(where, fmt.Sprintf("id = ANY($%d)", argPos))
		args = append(args, f.OrderIDs)
		argPos++
	}
	if f.FromTime != nil {
		where = append(where, fmt.Sprintf("created_at >= $%d", argPos))
		args = append(args, *f.FromTime)
		argPos++
	}
	if f.ToTime != nil {
		where = append(where, fmt.Sprintf("created_at <= $%d", argPos))
		args = append(args, *f.ToTime)
		argPos++
	}
	if f.MinAmount != nil {
		where = append(where, fmt.Sprintf("amount >= $%d", argPos))
		args = append(args, *f.MinAmount)
		argPos++
	}
	if f.MaxAmount != nil {
		where = append(where, fmt.Sprintf("amount <= $%d", argPos))
		args = append(args, *f.MaxAmount)
		argPos++
	}
	if f.CursorAt != nil && f.CursorID > 0 {
		where = append(where, fmt.Sprintf("(created_at < $%d OR (created_at = $%d AND id < $%d))", argPos, argPos, argPos+1))
		args = append(args, *f.CursorAt, f.CursorID)
		argPos += 2
	}
	whereSQL := strings.Join(where, " AND ")

	var total int64
	if f.IncludeTotal {
		countSQL := fmt.Sprintf("SELECT COUNT(*) FROM orders WHERE %s", whereSQL)
		if err := s.DB.QueryRow(ctx, countSQL, args...).Scan(&total); err != nil {
			return nil, 0, err
		}
	} else {
		total = -1
	}

	args = append(args, f.PageSize, offset)
	rows, err := s.DB.Query(ctx, fmt.Sprintf(`SELECT id,user_id,address,amount,status,created_at,
COALESCE((SELECT SUM(quantity) FROM order_items WHERE order_id=orders.id),0)::INT AS item_count
FROM orders WHERE %s ORDER BY created_at DESC, id DESC LIMIT $%d OFFSET $%d`, whereSQL, argPos, argPos+1), args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	items := make([]domain.Order, 0)
	for rows.Next() {
		var o domain.Order
		if err := rows.Scan(&o.ID, &o.UserID, &o.Address, &o.Amount, &o.Status, &o.CreatedAt, &o.ItemCount); err != nil {
			return nil, 0, err
		}
		items = append(items, o)
	}
	return items, total, nil
}

func normalizeOrderListFilter(f OrderListFilter) (OrderListFilter, int32) {
	if f.Page <= 0 {
		f.Page = 1
	}
	if f.PageSize <= 0 {
		f.PageSize = 20
	}
	if f.PageSize > 100 {
		f.PageSize = 100
	}
	offset := (f.Page - 1) * f.PageSize
	if f.CursorAt != nil && f.CursorID > 0 {
		offset = 0
	}
	return f, offset
}

func (s *Store) GetCachedOrderTotal(ctx context.Context, uid int64, status string) (int64, error) {
	if status == "" {
		var total int64
		if err := s.DB.QueryRow(ctx, `SELECT COALESCE(SUM(total),0) FROM user_order_totals WHERE user_id=$1`, uid).Scan(&total); err != nil {
			return 0, err
		}
		return total, nil
	}
	var total int64
	if err := s.DB.QueryRow(ctx, `SELECT COALESCE(total,0) FROM user_order_totals WHERE user_id=$1 AND status=$2`, uid, status).Scan(&total); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, nil
		}
		return 0, err
	}
	return total, nil
}

func (s *Store) EnqueueOrderStatsRefresh(ctx context.Context, userID int64) error {
	_, err := s.DB.Exec(ctx, `
INSERT INTO order_stats_refresh_queue(user_id, requested_at)
VALUES($1, NOW())
ON CONFLICT(user_id) DO UPDATE SET requested_at=EXCLUDED.requested_at`, userID)
	return err
}

func (s *Store) RefreshOrderStats(ctx context.Context, limit int32) (int32, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.DB.Query(ctx, `
SELECT user_id
FROM order_stats_refresh_queue
ORDER BY requested_at ASC
LIMIT $1`, limit)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	ids := make([]int64, 0, limit)
	for rows.Next() {
		var uid int64
		if err := rows.Scan(&uid); err != nil {
			return 0, err
		}
		ids = append(ids, uid)
	}
	processed := int32(0)
	for _, uid := range ids {
		tx, err := s.DB.Begin(ctx)
		if err != nil {
			return processed, err
		}
		if _, err := tx.Exec(ctx, `DELETE FROM user_order_totals WHERE user_id=$1`, uid); err != nil {
			tx.Rollback(ctx)
			return processed, err
		}
		if _, err := tx.Exec(ctx, `
INSERT INTO user_order_totals(user_id, status, total, updated_at)
SELECT user_id, status, COUNT(*), NOW()
FROM orders
WHERE user_id=$1
GROUP BY user_id, status`, uid); err != nil {
			tx.Rollback(ctx)
			return processed, err
		}
		if _, err := tx.Exec(ctx, `DELETE FROM order_stats_refresh_queue WHERE user_id=$1`, uid); err != nil {
			tx.Rollback(ctx)
			return processed, err
		}
		if err := tx.Commit(ctx); err != nil {
			return processed, err
		}
		processed++
	}
	return processed, nil
}

func (s *Store) GetOrderDetail(ctx context.Context, uid, orderID int64) (*domain.OrderDetail, error) {
	detail := &domain.OrderDetail{}
	err := s.DB.QueryRow(ctx, `SELECT id,user_id,address,amount,status,created_at FROM orders WHERE id=$1 AND user_id=$2`, orderID, uid).
		Scan(&detail.ID, &detail.UserID, &detail.Address, &detail.Amount, &detail.Status, &detail.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}

	rows, err := s.DB.Query(ctx, `SELECT product_id,product_title,price,quantity FROM order_items WHERE order_id=$1 ORDER BY id`, orderID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	detail.Items = make([]domain.OrderItem, 0)
	var itemCount int32
	for rows.Next() {
		var item domain.OrderItem
		if err := rows.Scan(&item.ProductID, &item.Title, &item.Price, &item.Quantity); err != nil {
			return nil, err
		}
		item.Subtotal = item.Price * float64(item.Quantity)
		detail.Items = append(detail.Items, item)
		itemCount += item.Quantity
	}
	detail.ItemCount = itemCount
	var idemKey string
	var idemCreated time.Time
	var idemLastReplay pgtype.Timestamptz
	idemErr := s.DB.QueryRow(ctx, `
SELECT idem_key, created_at, last_replay_at
FROM idempotency_keys
WHERE user_id=$1 AND status='completed' AND response_json->>'order_id' = $2
ORDER BY created_at DESC
LIMIT 1`, uid, fmt.Sprintf("%d", orderID)).Scan(&idemKey, &idemCreated, &idemLastReplay)
	if idemErr == nil {
		detail.IdempotencyKey = idemKey
		detail.IdempotencyCreatedAt = &idemCreated
		if idemLastReplay.Status == pgtype.Present {
			lastReplay := idemLastReplay.Time
			detail.IdempotencyLastReplayAt = &lastReplay
		}
	} else if !errors.Is(idemErr, pgx.ErrNoRows) {
		return nil, idemErr
	}
	return detail, nil
}

func (s *Store) CreateOrderFromCartWithIdempotency(ctx context.Context, uid int64, address, idemKey string) (*domain.Order, bool, error) {
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return nil, false, err
	}
	defer tx.Rollback(ctx)

	if idemKey != "" {
		var status string
		var response []byte
		err := tx.QueryRow(ctx, `SELECT status, response_json FROM idempotency_keys WHERE user_id=$1 AND idem_key=$2 FOR UPDATE`, uid, idemKey).Scan(&status, &response)
		if err == nil {
			if status == "completed" && len(response) > 0 {
				var cached domain.Order
				if unmarshalErr := json.Unmarshal(response, &cached); unmarshalErr == nil {
					if _, err := tx.Exec(ctx, `UPDATE idempotency_keys SET last_replay_at=NOW(), updated_at=NOW() WHERE user_id=$1 AND idem_key=$2`, uid, idemKey); err != nil {
						return nil, false, err
					}
					if err := tx.Commit(ctx); err != nil {
						return nil, false, err
					}
					return &cached, true, nil
				}
			}
			return nil, false, ErrIdempotencyInProgress
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return nil, false, err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO idempotency_keys(user_id, idem_key, status) VALUES($1,$2,'processing')`, uid, idemKey); err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.Code == "23505" {
				return nil, false, ErrIdempotencyInProgress
			}
			return nil, false, err
		}
	}

	rows, err := tx.Query(ctx, `SELECT ci.product_id,p.title,p.price,ci.quantity FROM cart_items ci JOIN products p ON p.id=ci.product_id WHERE ci.user_id=$1`, uid)
	if err != nil {
		return nil, false, err
	}
	items := make([]domain.CartItem, 0)
	amount := 0.0
	var itemCount int32
	for rows.Next() {
		var it domain.CartItem
		if err := rows.Scan(&it.ProductID, &it.Title, &it.Price, &it.Quantity); err != nil {
			rows.Close()
			return nil, false, err
		}
		amount += it.Price * float64(it.Quantity)
		itemCount += it.Quantity
		items = append(items, it)
	}
	rows.Close()
	if len(items) == 0 {
		return nil, false, ErrEmptyCart
	}

	var oid int64
	if err := tx.QueryRow(ctx, `INSERT INTO orders(user_id,address,amount,status) VALUES($1,$2,$3,'created') RETURNING id`, uid, address, amount).Scan(&oid); err != nil {
		return nil, false, err
	}
	for _, it := range items {
		if _, err := tx.Exec(ctx, `INSERT INTO order_items(order_id,product_id,product_title,price,quantity) VALUES($1,$2,$3,$4,$5)`, oid, it.ProductID, it.Title, it.Price, it.Quantity); err != nil {
			return nil, false, err
		}
	}
	if _, err := tx.Exec(ctx, `DELETE FROM cart_items WHERE user_id=$1`, uid); err != nil {
		return nil, false, err
	}
	if _, err := tx.Exec(ctx, `
INSERT INTO order_stats_refresh_queue(user_id, requested_at)
VALUES($1, NOW())
ON CONFLICT(user_id) DO UPDATE SET requested_at=EXCLUDED.requested_at`, uid); err != nil {
		return nil, false, err
	}

	order := &domain.Order{}
	if err := tx.QueryRow(ctx, `SELECT id,user_id,address,amount,status,created_at FROM orders WHERE id=$1`, oid).
		Scan(&order.ID, &order.UserID, &order.Address, &order.Amount, &order.Status, &order.CreatedAt); err != nil {
		return nil, false, err
	}
	order.ItemCount = itemCount
	payload, err := json.Marshal(map[string]any{
		"event":       "order.created",
		"order_id":    order.ID,
		"user_id":     order.UserID,
		"amount":      order.Amount,
		"status":      order.Status,
		"occurred_at": "",
	})
	if err != nil {
		return nil, false, err
	}
	traceID, commandID, replayJobID := traceMetadataFromContext(ctx)
	if _, err := tx.Exec(ctx, `
INSERT INTO outbox_events(topic, payload, status, next_attempt_at, max_retries, trace_id, command_id, correlation_source, replay_job_id)
VALUES($1,$2,'pending',NOW(),6,$3,$4,$5,NULLIF($6,0))`,
		"ecom.order.created", payload, traceID, commandID, "order_place", replayJobID); err != nil {
		return nil, false, err
	}

	if idemKey != "" {
		resp, err := json.Marshal(order)
		if err != nil {
			return nil, false, err
		}
		if _, err := tx.Exec(ctx, `UPDATE idempotency_keys SET status='completed', response_json=$1, updated_at=NOW() WHERE user_id=$2 AND idem_key=$3`, resp, uid, idemKey); err != nil {
			return nil, false, err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, false, err
	}
	return order, false, nil
}

func (s *Store) FetchPendingOutboxEvents(ctx context.Context, limit int32) ([]OutboxEvent, error) {
	rows, err := s.DB.Query(ctx, `
SELECT id, topic, payload, retry_count, max_retries, COALESCE(trace_id,''), COALESCE(command_id,''), COALESCE(correlation_source,''), COALESCE(replay_job_id,0), COALESCE(dead_letter_id,0)
FROM outbox_events
WHERE status='pending' AND next_attempt_at <= NOW()
ORDER BY id
LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]OutboxEvent, 0)
	for rows.Next() {
		var evt OutboxEvent
		if err := rows.Scan(&evt.ID, &evt.Topic, &evt.Payload, &evt.RetryCount, &evt.MaxRetries, &evt.TraceID, &evt.CommandID, &evt.CorrelationSource, &evt.ReplayJobID, &evt.DeadLetterID); err != nil {
			return nil, err
		}
		items = append(items, evt)
	}
	return items, nil
}

func (s *Store) MarkOutboxEventSent(ctx context.Context, eventID int64) error {
	_, err := s.DB.Exec(ctx, `UPDATE outbox_events SET status='sent', updated_at=NOW() WHERE id=$1`, eventID)
	return err
}

func (s *Store) MarkOutboxEventRetry(ctx context.Context, eventID int64, publishErr string) error {
	_, err := s.DB.Exec(ctx, `
UPDATE outbox_events
SET retry_count = retry_count + 1,
    last_error = $2,
    updated_at = NOW(),
    status = 'pending',
    next_attempt_at = NOW() + ((retry_count + 1) * 2) * INTERVAL '1 second'
WHERE id = $1`, eventID, publishErr)
	return err
}

func (s *Store) MoveOutboxEventToDeadLetter(ctx context.Context, evt OutboxEvent, publishErr string) error {
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, `
INSERT INTO dead_letter_events(source_outbox_id, topic, payload, last_error, retry_count)
VALUES($1,$2,$3,$4,$5)`, evt.ID, evt.Topic, evt.Payload, publishErr, evt.RetryCount+1); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
UPDATE outbox_events
SET retry_count = retry_count + 1,
    last_error = $2,
    status = 'dead_letter',
    next_attempt_at = NULL,
    updated_at = NOW()
WHERE id = $1`, evt.ID, publishErr); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Store) GetOutboxStats(ctx context.Context) (OutboxStats, error) {
	stats := OutboxStats{}
	rows, err := s.DB.Query(ctx, `SELECT status, COUNT(*) FROM outbox_events GROUP BY status`)
	if err != nil {
		return stats, err
	}
	defer rows.Close()
	for rows.Next() {
		var status string
		var count int64
		if err := rows.Scan(&status, &count); err != nil {
			return stats, err
		}
		switch status {
		case "pending":
			stats.Pending = count
		case "sent":
			stats.Sent = count
		case "failed":
			stats.Failed = count
		case "dead_letter":
			stats.DeadLetter = count
		}
	}
	if err := s.DB.QueryRow(ctx, `SELECT COUNT(*) FROM dead_letter_events`).Scan(&stats.TotalDLQ); err != nil {
		return stats, err
	}
	if err := s.DB.QueryRow(ctx, `SELECT COALESCE(SUM(retry_count),0) FROM outbox_events`).Scan(&stats.TotalRetries); err != nil {
		return stats, err
	}
	return stats, nil
}

func (s *Store) ListDeadLetterEvents(ctx context.Context, limit int32) ([]DeadLetterEvent, error) {
	if limit <= 0 {
		limit = 20
	}
	rows, err := s.DB.Query(ctx, `
SELECT id, topic, payload, COALESCE(last_error,''), retry_count, moved_at
FROM dead_letter_events
WHERE replay_status IN ('pending', 'failed')
ORDER BY moved_at DESC
LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]DeadLetterEvent, 0, limit)
	for rows.Next() {
		var it DeadLetterEvent
		if err := rows.Scan(&it.ID, &it.Topic, &it.Payload, &it.LastError, &it.RetryCount, &it.MovedAt); err != nil {
			return nil, err
		}
		items = append(items, it)
	}
	return items, nil
}

func (s *Store) ReplayDeadLetterByID(ctx context.Context, deadLetterID int64) error {
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var evt DeadLetterEvent
	err = tx.QueryRow(ctx, `
SELECT id, topic, payload, COALESCE(last_error,''), retry_count, moved_at
FROM dead_letter_events
WHERE id=$1
FOR UPDATE`, deadLetterID).Scan(&evt.ID, &evt.Topic, &evt.Payload, &evt.LastError, &evt.RetryCount, &evt.MovedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	traceID, commandID, replayJobID := traceMetadataFromContext(ctx)

	if _, err := tx.Exec(ctx, `
INSERT INTO outbox_events(topic, payload, status, retry_count, max_retries, next_attempt_at, created_at, updated_at, trace_id, command_id, correlation_source, replay_job_id, dead_letter_id)
VALUES($1,$2,'pending',0,6,NOW(),NOW(),NOW(),$3,$4,$5,NULLIF($6,0),$7)`,
		evt.Topic, evt.Payload, traceID, commandID, "replay_job", replayJobID, deadLetterID); err != nil {
		_, _ = tx.Exec(ctx, `UPDATE dead_letter_events SET replay_status='failed', replay_count=replay_count+1, last_replay_error=$2, last_replay_at=NOW() WHERE id=$1`, deadLetterID, err.Error())
		return err
	}

	if _, err := tx.Exec(ctx, `
UPDATE dead_letter_events
SET replay_status='replayed',
    replay_count=replay_count+1,
    last_replay_error='',
    last_replay_at=NOW()
WHERE id=$1`, deadLetterID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Store) CreateReplayJob(ctx context.Context, actorUserID int64, topic string, limit int32) (int64, int32, error) {
	if limit <= 0 {
		limit = 50
	}
	if limit > 500 {
		limit = 500
	}
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return 0, 0, err
	}
	defer tx.Rollback(ctx)
	traceID, commandID, _ := traceMetadataFromContext(ctx)

	var jobID int64
	if err := tx.QueryRow(ctx, `
INSERT INTO replay_jobs(created_by, status, topic_filter, trace_id, command_id)
VALUES($1,'queued',$2,$3,$4)
RETURNING id`, actorUserID, topic, traceID, commandID).Scan(&jobID); err != nil {
		return 0, 0, err
	}

	query := `
SELECT id
FROM dead_letter_events
WHERE replay_status IN ('pending','failed')`
	args := []any{}
	if topic != "" {
		query += ` AND topic=$1`
		args = append(args, topic)
	}
	query += ` ORDER BY moved_at DESC LIMIT `
	if topic != "" {
		query += `$2`
		args = append(args, limit)
	} else {
		query += `$1`
		args = append(args, limit)
	}
	rows, err := tx.Query(ctx, query, args...)
	if err != nil {
		return 0, 0, err
	}
	deadLetterIDs := make([]int64, 0, limit)
	for rows.Next() {
		var deadLetterID int64
		if err := rows.Scan(&deadLetterID); err != nil {
			rows.Close()
			return 0, 0, err
		}
		deadLetterIDs = append(deadLetterIDs, deadLetterID)
	}
	rows.Close()

	var total int32
	for _, deadLetterID := range deadLetterIDs {
		if _, err := tx.Exec(ctx, `
INSERT INTO replay_job_items(job_id, dead_letter_id, status)
VALUES($1,$2,'pending')
ON CONFLICT(job_id, dead_letter_id) DO NOTHING`, jobID, deadLetterID); err != nil {
			return 0, 0, err
		}
		if _, err := tx.Exec(ctx, `
UPDATE dead_letter_events
SET replay_status='queued'
WHERE id=$1`, deadLetterID); err != nil {
			return 0, 0, err
		}
		total++
	}
	if _, err := tx.Exec(ctx, `UPDATE replay_jobs SET total_items=$2, updated_at=NOW() WHERE id=$1`, jobID, total); err != nil {
		return 0, 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, 0, err
	}
	return jobID, total, nil
}

func (s *Store) GetReplayJob(ctx context.Context, jobID int64) (*ReplayJob, []ReplayFailedGroup, error) {
	job := &ReplayJob{}
	if err := s.DB.QueryRow(ctx, `
SELECT id,status,topic_filter,COALESCE(trace_id,''),COALESCE(command_id,''),total_items,processed_items,success_items,failed_items,last_error,created_at,updated_at
FROM replay_jobs
WHERE id=$1`, jobID).Scan(
		&job.ID, &job.Status, &job.TopicFilter, &job.TraceID, &job.CommandID, &job.TotalItems, &job.ProcessedItems,
		&job.SuccessItems, &job.FailedItems, &job.LastError, &job.CreatedAt, &job.UpdatedAt,
	); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil, ErrNotFound
		}
		return nil, nil, err
	}

	rows, err := s.DB.Query(ctx, `
SELECT error_group, COUNT(*)::int
FROM replay_job_items
WHERE job_id=$1 AND status='failed' AND error_group <> ''
GROUP BY error_group
ORDER BY COUNT(*) DESC, error_group`, jobID)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	groups := make([]ReplayFailedGroup, 0)
	for rows.Next() {
		var g ReplayFailedGroup
		if err := rows.Scan(&g.ErrorGroup, &g.Count); err != nil {
			return nil, nil, err
		}
		groups = append(groups, g)
	}
	return job, groups, nil
}

func (s *Store) RecoverExpiredReplayLeases(ctx context.Context) error {
	_, err := s.DB.Exec(ctx, `
UPDATE replay_jobs
SET status='queued', lease_owner='', lease_expires_at=NULL, updated_at=NOW()
WHERE status='running' AND lease_expires_at IS NOT NULL AND lease_expires_at < NOW()`)
	return err
}

func (s *Store) PickQueuedReplayJob(ctx context.Context, leaseOwner string, leaseDuration time.Duration) (*ReplayJob, error) {
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	job := &ReplayJob{}
	err = tx.QueryRow(ctx, `
SELECT id,status,topic_filter,COALESCE(trace_id,''),COALESCE(command_id,''),total_items,processed_items,success_items,failed_items,last_error,created_at,updated_at
FROM replay_jobs
WHERE status IN ('queued','running')
ORDER BY created_at ASC
LIMIT 1
FOR UPDATE SKIP LOCKED`).Scan(
		&job.ID, &job.Status, &job.TopicFilter, &job.TraceID, &job.CommandID, &job.TotalItems, &job.ProcessedItems,
		&job.SuccessItems, &job.FailedItems, &job.LastError, &job.CreatedAt, &job.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if job.Status == "running" && job.UpdatedAt.After(time.Now().Add(-2*leaseDuration)) {
		return nil, ErrNotFound
	}
	if _, err := tx.Exec(ctx, `UPDATE replay_jobs SET status='running', lease_owner=$2, lease_expires_at=$3, updated_at=NOW() WHERE id=$1`, job.ID, leaseOwner, time.Now().Add(leaseDuration)); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	job.Status = "running"
	return job, nil
}

func (s *Store) FetchPendingReplayItems(ctx context.Context, jobID int64, limit int32) ([]int64, error) {
	rows, err := s.DB.Query(ctx, `
SELECT dead_letter_id
FROM replay_job_items
WHERE job_id=$1 AND status='pending' AND terminal=FALSE AND next_attempt_at <= NOW() AND attempts < max_attempts
ORDER BY id
LIMIT $2`, jobID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ids := make([]int64, 0, limit)
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, nil
}

func (s *Store) MarkReplayItemSuccess(ctx context.Context, jobID, deadLetterID int64) error {
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `
UPDATE replay_job_items
SET status='success', attempts=attempts+1, last_error='', error_group='', terminal=FALSE, next_attempt_at=NOW(), updated_at=NOW()
WHERE job_id=$1 AND dead_letter_id=$2`, jobID, deadLetterID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
UPDATE replay_jobs
SET processed_items=processed_items+1, success_items=success_items+1, updated_at=NOW()
WHERE id=$1`, jobID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Store) MarkReplayItemFailed(ctx context.Context, jobID, deadLetterID int64, errMsg string) error {
	if len(errMsg) > 240 {
		errMsg = errMsg[:240]
	}
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var attempts int32
	var maxAttempts int32
	if err := tx.QueryRow(ctx, `SELECT attempts, max_attempts FROM replay_job_items WHERE job_id=$1 AND dead_letter_id=$2 FOR UPDATE`, jobID, deadLetterID).Scan(&attempts, &maxAttempts); err != nil {
		return err
	}
	nextAttempts := attempts + 1
	backoffSeconds := int64(1) << minInt32(nextAttempts, 6)
	nextAttemptAt := time.Now().Add(time.Duration(backoffSeconds) * time.Second)
	terminal := nextAttempts >= maxAttempts
	nextStatus := "pending"
	if terminal {
		nextStatus = "failed"
	}
	if _, err := tx.Exec(ctx, `
UPDATE replay_job_items
SET status=$3,
    attempts=$4,
    last_error=$5,
    error_group=$5,
    terminal=$6,
    next_attempt_at=$7,
    updated_at=NOW()
WHERE job_id=$1 AND dead_letter_id=$2`, jobID, deadLetterID, nextStatus, nextAttempts, errMsg, terminal, nextAttemptAt); err != nil {
		return err
	}
	if terminal {
		if _, err := tx.Exec(ctx, `
UPDATE replay_jobs
SET processed_items=processed_items+1, failed_items=failed_items+1, last_error=$2, updated_at=NOW()
WHERE id=$1`, jobID, errMsg); err != nil {
			return err
		}
	} else {
		if _, err := tx.Exec(ctx, `
UPDATE replay_jobs
SET last_error=$2, updated_at=NOW()
WHERE id=$1`, jobID, errMsg); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func (s *Store) FinalizeReplayJob(ctx context.Context, jobID int64) error {
	_, err := s.DB.Exec(ctx, `
UPDATE replay_jobs
SET status = CASE
  WHEN failed_items = 0 THEN 'completed'
  WHEN success_items = 0 THEN 'failed'
  ELSE 'partial'
END,
lease_owner='',
lease_expires_at=NULL,
updated_at = NOW()
WHERE id=$1`, jobID)
	return err
}

func (s *Store) RetryFailedReplayItems(ctx context.Context, jobID int64, errorGroup string, limit int32, resetAttempts bool) (RetryFailedReplaySummary, error) {
	summary := RetryFailedReplaySummary{
		ErrorGroupsBefore: map[string]int32{},
		ErrorGroupsAfter:  map[string]int32{},
	}
	if limit <= 0 {
		limit = 100
	}
	traceID, commandID, _ := traceMetadataFromContext(ctx)
	var rows pgx.Rows
	var err error
	if errorGroup == "" {
		rows, err = s.DB.Query(ctx, `
SELECT dead_letter_id, attempts, COALESCE(error_group, '')
FROM replay_job_items
WHERE job_id=$1 AND status='failed'
ORDER BY id
LIMIT $2`, jobID, limit)
	} else {
		rows, err = s.DB.Query(ctx, `
SELECT dead_letter_id, attempts, COALESCE(error_group, '')
FROM replay_job_items
WHERE job_id=$1 AND status='failed' AND error_group=$2
ORDER BY id
LIMIT $3`, jobID, errorGroup, limit)
	}
	if err != nil {
		return summary, err
	}
	defer rows.Close()

	for rows.Next() {
		var deadID int64
		var attemptsBefore int32
		var groupBefore string
		if err := rows.Scan(&deadID, &attemptsBefore, &groupBefore); err != nil {
			return summary, err
		}
		summary.Retried++
		summary.AttemptsBeforeTotal += int64(attemptsBefore)
		summary.ErrorGroupsBefore[groupBefore]++

		if resetAttempts {
			if _, err := s.DB.Exec(ctx, `
UPDATE replay_job_items
SET status='pending', terminal=FALSE, attempts=0, next_attempt_at=NOW(), last_error='', error_group='', updated_at=NOW()
WHERE job_id=$1 AND dead_letter_id=$2`, jobID, deadID); err != nil {
				return summary, err
			}
			summary.ErrorGroupsAfter[""]++
		} else if _, err := s.DB.Exec(ctx, `
UPDATE replay_job_items
SET status='pending', terminal=FALSE, next_attempt_at=NOW(), updated_at=NOW()
WHERE job_id=$1 AND dead_letter_id=$2`, jobID, deadID); err != nil {
			return summary, err
		} else {
			summary.AttemptsAfterTotal += int64(attemptsBefore)
			summary.ErrorGroupsAfter[groupBefore]++
		}
	}
	if rows.Err() != nil {
		return summary, rows.Err()
	}
	if summary.Retried > 0 {
		_, _ = s.DB.Exec(ctx, `
UPDATE replay_jobs
SET status='queued',
    processed_items=GREATEST(processed_items-$2, 0),
    failed_items=GREATEST(failed_items-$2, 0),
    trace_id=COALESCE(NULLIF($3,''), trace_id),
    command_id=COALESCE(NULLIF($4,''), command_id),
    updated_at=NOW()
WHERE id=$1`, jobID, summary.Retried, traceID, commandID)
	}
	return summary, nil
}

func (s *Store) SearchReplayJobsByCorrelation(ctx context.Context, traceID, commandID string, limit int32, cursorID int64) ([]ReplayJob, error) {
	traceID = strings.TrimSpace(traceID)
	commandID = strings.TrimSpace(commandID)
	if limit <= 0 {
		limit = 20
	}
	if limit > 200 {
		limit = 200
	}
	if traceID == "" && commandID == "" {
		return []ReplayJob{}, nil
	}
	query := `
SELECT id,status,topic_filter,COALESCE(trace_id,''),COALESCE(command_id,''),total_items,processed_items,success_items,failed_items,last_error,created_at,updated_at
FROM replay_jobs
WHERE 1=1`
	args := make([]any, 0, 3)
	idx := 1
	if traceID != "" {
		query += fmt.Sprintf(" AND trace_id=$%d", idx)
		args = append(args, traceID)
		idx++
	}
	if commandID != "" {
		query += fmt.Sprintf(" AND command_id=$%d", idx)
		args = append(args, commandID)
		idx++
	}
	if cursorID > 0 {
		query += fmt.Sprintf(" AND id < $%d", idx)
		args = append(args, cursorID)
		idx++
	}
	query += fmt.Sprintf(" ORDER BY id DESC LIMIT $%d", idx)
	args = append(args, limit)
	rows, err := s.DB.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]ReplayJob, 0, limit)
	for rows.Next() {
		var r ReplayJob
		if err := rows.Scan(&r.ID, &r.Status, &r.TopicFilter, &r.TraceID, &r.CommandID, &r.TotalItems, &r.ProcessedItems, &r.SuccessItems, &r.FailedItems, &r.LastError, &r.CreatedAt, &r.UpdatedAt); err != nil {
			return nil, err
		}
		items = append(items, r)
	}
	return items, nil
}

func (s *Store) SearchOutboxEventsByCorrelation(ctx context.Context, traceID, commandID string, limit int32, cursorID int64) ([]OutboxEventRecord, error) {
	traceID = strings.TrimSpace(traceID)
	commandID = strings.TrimSpace(commandID)
	if limit <= 0 {
		limit = 20
	}
	if limit > 200 {
		limit = 200
	}
	if traceID == "" && commandID == "" {
		return []OutboxEventRecord{}, nil
	}
	query := `
SELECT id,topic,status,retry_count,max_retries,COALESCE(trace_id,''),COALESCE(command_id,''),COALESCE(correlation_source,''),COALESCE(replay_job_id,0),COALESCE(dead_letter_id,0),created_at,updated_at
FROM outbox_events
WHERE 1=1`
	args := make([]any, 0, 3)
	idx := 1
	if traceID != "" {
		query += fmt.Sprintf(" AND trace_id=$%d", idx)
		args = append(args, traceID)
		idx++
	}
	if commandID != "" {
		query += fmt.Sprintf(" AND command_id=$%d", idx)
		args = append(args, commandID)
		idx++
	}
	if cursorID > 0 {
		query += fmt.Sprintf(" AND id < $%d", idx)
		args = append(args, cursorID)
		idx++
	}
	query += fmt.Sprintf(" ORDER BY id DESC LIMIT $%d", idx)
	args = append(args, limit)
	rows, err := s.DB.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]OutboxEventRecord, 0, limit)
	for rows.Next() {
		var r OutboxEventRecord
		if err := rows.Scan(&r.ID, &r.Topic, &r.Status, &r.RetryCount, &r.MaxRetries, &r.TraceID, &r.CommandID, &r.CorrelationSource, &r.ReplayJobID, &r.DeadLetterID, &r.CreatedAt, &r.UpdatedAt); err != nil {
			return nil, err
		}
		items = append(items, r)
	}
	return items, nil
}

func minInt32(a, b int32) int32 {
	if a < b {
		return a
	}
	return b
}

func (s *Store) GetOutboxOldestPendingAgeSeconds(ctx context.Context) (float64, error) {
	var ageSeconds float64
	if err := s.DB.QueryRow(ctx, `
SELECT COALESCE(EXTRACT(EPOCH FROM (NOW() - MIN(created_at))), 0)
FROM outbox_events
WHERE status='pending'`).Scan(&ageSeconds); err != nil {
		return 0, err
	}
	if ageSeconds < 0 {
		ageSeconds = 0
	}
	return ageSeconds, nil
}
