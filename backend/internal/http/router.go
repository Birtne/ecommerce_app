package http

import (
	"context"
	"encoding/base64"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/ductor/ecommerce_app/backend/internal/domain"
	"github.com/ductor/ecommerce_app/backend/internal/metrics"
	"github.com/ductor/ecommerce_app/backend/internal/repository"
	"github.com/ductor/ecommerce_app/backend/internal/service"
)

type orderService interface {
	PlaceOrder(ctx context.Context, uid int64, address, idemKey string) (*domain.Order, bool, error)
	GetOrderDetail(ctx context.Context, uid, orderID int64) (*domain.OrderDetail, error)
	ListOrders(ctx context.Context, uid int64, f service.OrderListFilter) (*service.OrderListResult, error)
}

type Handler struct {
	authSvc         *service.AuthService
	productSvc      *service.ProductService
	cartSvc         *service.CartService
	orderSvc        orderService
	replayJobSvc    *service.ReplayJobService
	store           *repository.Store
	outboxPublisher *service.OutboxPublisher
}

type adminCommandWaitOptions struct {
	MaxWait        time.Duration
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
}

var (
	adminWaitOptsOnce    sync.Once
	adminWaitOpts        adminCommandWaitOptions
	allowedOrderStatuses = map[string]struct{}{
		"created":    {},
		"processing": {},
		"shipped":    {},
		"completed":  {},
		"cancelled":  {},
		"failed":     {},
	}
)

func getAdminCommandWaitOptions() adminCommandWaitOptions {
	adminWaitOptsOnce.Do(func() {
		adminWaitOpts = adminCommandWaitOptions{
			MaxWait:        5 * time.Second,
			InitialBackoff: 20 * time.Millisecond,
			MaxBackoff:     320 * time.Millisecond,
		}
		if ms, err := strconv.Atoi(strings.TrimSpace(os.Getenv("ADMIN_COMMAND_WAIT_TIMEOUT_MS"))); err == nil && ms >= 200 {
			adminWaitOpts.MaxWait = time.Duration(ms) * time.Millisecond
		}
		if ms, err := strconv.Atoi(strings.TrimSpace(os.Getenv("ADMIN_COMMAND_WAIT_INITIAL_BACKOFF_MS"))); err == nil && ms >= 5 {
			adminWaitOpts.InitialBackoff = time.Duration(ms) * time.Millisecond
		}
		if ms, err := strconv.Atoi(strings.TrimSpace(os.Getenv("ADMIN_COMMAND_WAIT_MAX_BACKOFF_MS"))); err == nil && ms >= 20 {
			adminWaitOpts.MaxBackoff = time.Duration(ms) * time.Millisecond
		}
	})
	return adminWaitOpts
}

func NewHandler(auth *service.AuthService, product *service.ProductService, cart *service.CartService, order *service.OrderService, replay *service.ReplayJobService, store *repository.Store, outbox *service.OutboxPublisher) *Handler {
	metrics.Init()
	return &Handler{authSvc: auth, productSvc: product, cartSvc: cart, orderSvc: order, replayJobSvc: replay, store: store, outboxPublisher: outbox}
}

func RegisterRoutes(h *server.Hertz, hd *Handler) {
	h.GET("/health", func(ctx context.Context, c *app.RequestContext) {
		c.JSON(http.StatusOK, map[string]string{"status": "ok"})
	})
	h.GET("/health/outbox", hd.outboxHealth)
	h.GET("/metrics", hd.metrics)

	v1 := h.Group("/api/v1")
	v1.POST("/auth/register", hd.register)
	v1.POST("/auth/login", hd.login)
	v1.GET("/products", hd.listProducts)

	auth := v1.Group("", hd.authMiddleware)
	auth.GET("/cart", hd.getCart)
	auth.POST("/cart/items", hd.addCartItem)
	auth.DELETE("/cart/items/:product_id", hd.removeCartItem)
	auth.POST("/orders", hd.placeOrder)
	auth.GET("/orders", hd.listOrders)
	auth.GET("/orders/:order_id", hd.getOrder)

	v1.POST("/admin/auth/login", hd.adminLogin)
	v1.POST("/admin/auth/logout", hd.adminLogout)
	admin := v1.Group("/admin", hd.adminSessionMiddleware)
	admin.POST("/outbox/replay-jobs", hd.createReplayJob)
	admin.GET("/outbox/replay-jobs/:job_id", hd.getReplayJob)
	admin.POST("/outbox/replay-jobs/:job_id/retry-failed", hd.retryFailedReplayJob)
	admin.GET("/trace/replay-jobs", hd.searchReplayJobsByTrace)
	admin.GET("/trace/outbox-events", hd.searchOutboxEventsByTrace)
	admin.GET("/audit-logs", hd.listAuditLogs)
	admin.GET("/audit-logs/export", hd.exportAuditLogs)
}

func (h *Handler) outboxHealth(ctx context.Context, c *app.RequestContext) {
	stats, err := h.store.GetOutboxStats(ctx)
	if err != nil {
		c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	runtime := h.outboxPublisher.RuntimeMetrics()
	c.JSON(http.StatusOK, map[string]any{"db_stats": stats, "runtime_stats": runtime})
}

func (h *Handler) metrics(ctx context.Context, c *app.RequestContext) {
	stats, err := h.store.GetOutboxStats(ctx)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	oldestPendingAge, err := h.store.GetOutboxOldestPendingAgeSeconds(ctx)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	runtime := h.outboxPublisher.RuntimeMetrics()

	metrics.OutboxEventsGauge.WithLabelValues("pending").Set(float64(stats.Pending))
	metrics.OutboxEventsGauge.WithLabelValues("sent").Set(float64(stats.Sent))
	metrics.OutboxEventsGauge.WithLabelValues("failed").Set(float64(stats.Failed))
	metrics.OutboxEventsGauge.WithLabelValues("dead_letter").Set(float64(stats.DeadLetter))
	metrics.OutboxEventsGauge.WithLabelValues("dead_letter_rows").Set(float64(stats.TotalDLQ))
	metrics.OutboxEventsGauge.WithLabelValues("total_retries").Set(float64(stats.TotalRetries))

	metrics.OutboxRuntimeGauge.WithLabelValues("runs").Set(float64(runtime.Runs))
	metrics.OutboxRuntimeGauge.WithLabelValues("attempts").Set(float64(runtime.Attempts))
	metrics.OutboxRuntimeGauge.WithLabelValues("sent").Set(float64(runtime.Sent))
	metrics.OutboxRuntimeGauge.WithLabelValues("retried").Set(float64(runtime.Retried))
	metrics.OutboxRuntimeGauge.WithLabelValues("dead_lettered").Set(float64(runtime.DeadLettered))
	metrics.OutboxRuntimeGauge.WithLabelValues("last_run_unix").Set(float64(runtime.LastRunUnix))
	metrics.OutboxRuntimeGauge.WithLabelValues("oldest_pending_age_seconds").Set(oldestPendingAge)

	body, err := metrics.EncodeText()
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	c.Header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	c.String(http.StatusOK, body)
}

func (h *Handler) authMiddleware(ctx context.Context, c *app.RequestContext) {
	parts := strings.Split(string(c.GetHeader("Authorization")), " ")
	if len(parts) != 2 || parts[0] != "Bearer" {
		c.AbortWithStatusJSON(http.StatusUnauthorized, map[string]string{"error": "missing bearer token"})
		return
	}
	u, err := h.authSvc.ResolveToken(ctx, parts[1])
	if err != nil {
		c.AbortWithStatusJSON(http.StatusUnauthorized, map[string]string{"error": "invalid token"})
		return
	}
	c.Set("user_id", u.ID)
	c.Next(ctx)
}

func (h *Handler) adminSessionMiddleware(ctx context.Context, c *app.RequestContext) {
	parts := strings.Split(string(c.GetHeader("Authorization")), " ")
	if len(parts) != 2 || parts[0] != "Bearer" {
		c.AbortWithStatusJSON(http.StatusUnauthorized, map[string]string{"error": "missing bearer token"})
		return
	}
	u, err := h.authSvc.ResolveAdminToken(ctx, parts[1])
	if err != nil {
		c.AbortWithStatusJSON(http.StatusUnauthorized, map[string]string{"error": "invalid admin session"})
		return
	}
	c.Set("admin_user_id", u.ID)
	c.Set("admin_role", u.Role)
	c.Next(ctx)
}

func (h *Handler) register(ctx context.Context, c *app.RequestContext) {
	var req struct {
		Email    string `json:"email"`
		Password string `json:"password"`
		Name     string `json:"name"`
	}
	if err := c.Bind(&req); err != nil {
		c.JSON(http.StatusBadRequest, map[string]string{"error": "invalid payload"})
		return
	}
	uid, tok, err := h.authSvc.Register(ctx, req.Email, req.Password, req.Name)
	if err != nil {
		c.JSON(http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, map[string]any{"user_id": uid, "token": tok})
}

func (h *Handler) login(ctx context.Context, c *app.RequestContext) {
	var req struct {
		Email    string `json:"email"`
		Password string `json:"password"`
	}
	if err := c.Bind(&req); err != nil {
		c.JSON(http.StatusBadRequest, map[string]string{"error": "invalid payload"})
		return
	}
	uid, tok, err := h.authSvc.Login(ctx, req.Email, req.Password)
	if err != nil {
		c.JSON(http.StatusUnauthorized, map[string]string{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, map[string]any{"user_id": uid, "token": tok})
}

func (h *Handler) adminLogin(ctx context.Context, c *app.RequestContext) {
	var req struct {
		Email    string `json:"email"`
		Password string `json:"password"`
	}
	if err := c.Bind(&req); err != nil {
		c.JSON(http.StatusBadRequest, map[string]string{"error": "invalid payload"})
		return
	}
	uid, token, err := h.authSvc.AdminLogin(ctx, req.Email, req.Password)
	if err != nil {
		c.JSON(http.StatusUnauthorized, map[string]string{"error": err.Error()})
		return
	}
	_ = h.store.InsertAuditLog(ctx, uid, "admin_login", "session", token, map[string]any{"email": req.Email})
	c.JSON(http.StatusOK, map[string]any{"admin_user_id": uid, "token": token})
}

func (h *Handler) adminLogout(ctx context.Context, c *app.RequestContext) {
	parts := strings.Split(string(c.GetHeader("Authorization")), " ")
	if len(parts) != 2 || parts[0] != "Bearer" {
		c.JSON(http.StatusUnauthorized, map[string]string{"error": "missing bearer token"})
		return
	}
	admin, err := h.authSvc.ResolveAdminToken(ctx, parts[1])
	if err != nil {
		c.JSON(http.StatusUnauthorized, map[string]string{"error": "invalid admin session"})
		return
	}
	if err := h.authSvc.AdminLogout(ctx, parts[1]); err != nil {
		c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	_ = h.store.InsertAuditLog(ctx, admin.ID, "admin_logout", "session", parts[1], map[string]any{})
	c.JSON(http.StatusOK, map[string]any{"ok": true})
}

func (h *Handler) listProducts(ctx context.Context, c *app.RequestContext) {
	items, err := h.productSvc.ListProducts(ctx)
	if err != nil {
		c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, items)
}

func userIDFromContext(c *app.RequestContext) (int64, error) {
	v, ok := c.Get("user_id")
	if !ok {
		return 0, repository.ErrNotFound
	}
	uid, ok := v.(int64)
	if !ok {
		return 0, repository.ErrNotFound
	}
	return uid, nil
}

func adminUserIDFromContext(c *app.RequestContext) (int64, error) {
	v, ok := c.Get("admin_user_id")
	if !ok {
		return 0, repository.ErrNotFound
	}
	uid, ok := v.(int64)
	if !ok {
		return 0, repository.ErrNotFound
	}
	return uid, nil
}

func adminRoleFromContext(c *app.RequestContext) string {
	v, ok := c.Get("admin_role")
	if !ok {
		return ""
	}
	role, _ := v.(string)
	return role
}

func commandIDFromHeader(c *app.RequestContext) string {
	return strings.TrimSpace(string(c.GetHeader("X-Command-Id")))
}

func traceIDFromHeader(c *app.RequestContext) string {
	v := strings.TrimSpace(string(c.GetHeader("X-Trace-Id")))
	if v != "" {
		return v
	}
	return strings.TrimSpace(string(c.GetHeader("X-Request-Id")))
}

func logAdminCommandWait(level, msg string, fields map[string]any) {
	payload := map[string]any{
		"component": "admin_command_wait",
		"level":     level,
		"msg":       msg,
		"ts":        time.Now().UTC().Format(time.RFC3339),
	}
	for k, v := range fields {
		payload[k] = v
	}
	if b, err := json.Marshal(payload); err == nil {
		log.Printf("%s", b)
		return
	}
	log.Printf("admin_command_wait level=%s msg=%s", level, msg)
}

func (h *Handler) waitAdminCommandResult(ctx context.Context, actorUserID int64, action, commandID, traceID string) ([]byte, bool, error) {
	opts := getAdminCommandWaitOptions()
	start := time.Now()
	deadline := start.Add(opts.MaxWait)
	backoff := opts.InitialBackoff

	record := func(result string) {
		metrics.AdminCommandWaitTotal.WithLabelValues(action, result).Inc()
		cost := time.Since(start)
		metrics.AdminCommandWaitDuration.WithLabelValues(action, result).Observe(cost.Seconds())
		if result == "ok" || result == "timeout" || result == "canceled" {
			logAdminCommandWait("info", "admin command wait result", map[string]any{
				"action":             action,
				"command_id":         commandID,
				"trace_id":           traceID,
				"wait_result":        result,
				"wait_ms":            cost.Milliseconds(),
				"actor_user_id":      actorUserID,
				"max_wait_ms":        opts.MaxWait.Milliseconds(),
				"initial_backoff_ms": opts.InitialBackoff.Milliseconds(),
				"max_backoff_ms":     opts.MaxBackoff.Milliseconds(),
			})
		}
	}

	for {
		if time.Now().After(deadline) {
			record("timeout")
			return nil, false, nil
		}
		body, ok, err := h.store.GetAdminCommandResult(ctx, actorUserID, action, commandID)
		if err != nil {
			record("error")
			return nil, false, err
		}
		if ok {
			record("ok")
			return body, true, nil
		}
		wait := backoff
		if now := time.Now(); now.Add(wait).After(deadline) {
			wait = time.Until(deadline)
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			record("canceled")
			return nil, false, ctx.Err()
		case <-timer.C:
		}
		if backoff < opts.MaxBackoff {
			backoff *= 2
			if backoff > opts.MaxBackoff {
				backoff = opts.MaxBackoff
			}
		}
	}
}

func (h *Handler) requireAdminPermission(ctx context.Context, c *app.RequestContext, action string) bool {
	role := adminRoleFromContext(c)
	allowed, err := h.authSvc.AdminHasPermission(ctx, role, action)
	if err != nil {
		c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return false
	}
	if !allowed {
		c.JSON(http.StatusForbidden, map[string]string{"error": "forbidden"})
		return false
	}
	return true
}

func (h *Handler) getCart(ctx context.Context, c *app.RequestContext) {
	uid, err := userIDFromContext(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
		return
	}
	items, total, err := h.cartSvc.GetCart(ctx, uid)
	if err != nil {
		c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, map[string]any{"items": items, "total_amount": total})
}

func (h *Handler) addCartItem(ctx context.Context, c *app.RequestContext) {
	uid, err := userIDFromContext(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
		return
	}
	var req struct {
		ProductID int64 `json:"product_id"`
		Quantity  int32 `json:"quantity"`
	}
	if err := c.Bind(&req); err != nil {
		c.JSON(http.StatusBadRequest, map[string]string{"error": "invalid payload"})
		return
	}
	if err := validateCartItemInput(req.ProductID, req.Quantity); err != nil {
		c.JSON(http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if err := h.cartSvc.AddOrUpdateItem(ctx, uid, req.ProductID, req.Quantity); err != nil {
		c.JSON(http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, map[string]bool{"ok": true})
}

func (h *Handler) removeCartItem(ctx context.Context, c *app.RequestContext) {
	uid, err := userIDFromContext(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
		return
	}
	pid, err := strconv.ParseInt(c.Param("product_id"), 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, map[string]string{"error": "invalid product_id"})
		return
	}
	if err := h.cartSvc.RemoveItem(ctx, uid, pid); err != nil {
		c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, map[string]bool{"ok": true})
}

func (h *Handler) placeOrder(ctx context.Context, c *app.RequestContext) {
	uid, err := userIDFromContext(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
		return
	}
	idemKey := strings.TrimSpace(string(c.GetHeader("Idempotency-Key")))
	if err := validateIdempotencyKey(idemKey); err != nil {
		c.JSON(http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	var req struct {
		Address string `json:"address"`
	}
	if err := c.Bind(&req); err != nil {
		c.JSON(http.StatusBadRequest, map[string]string{"error": "invalid payload"})
		return
	}
	address, err := normalizeAddress(req.Address)
	if err != nil {
		c.JSON(http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	traceID := traceIDFromHeader(c)
	orderCtx := repository.WithTraceMetadata(ctx, traceID, idemKey)
	order, replay, err := h.orderSvc.PlaceOrder(orderCtx, uid, address, idemKey)
	if err != nil {
		code := http.StatusInternalServerError
		if err == repository.ErrEmptyCart || err == repository.ErrIdempotencyInProgress {
			code = http.StatusBadRequest
		}
		c.JSON(code, map[string]string{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, map[string]any{"order_id": order.ID, "user_id": order.UserID, "address": order.Address, "amount": order.Amount, "status": order.Status, "idempotent_replay": replay})
}

func (h *Handler) getOrder(ctx context.Context, c *app.RequestContext) {
	uid, err := userIDFromContext(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
		return
	}
	orderID, err := strconv.ParseInt(c.Param("order_id"), 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, map[string]string{"error": "invalid order_id"})
		return
	}
	detail, err := h.orderSvc.GetOrderDetail(ctx, uid, orderID)
	if err != nil {
		if err == repository.ErrNotFound {
			c.JSON(http.StatusNotFound, map[string]string{"error": "order not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, detail)
}

func parseOrderCursor(raw string) (*time.Time, int64, error) {
	decoded, err := base64.StdEncoding.DecodeString(raw)
	if err != nil {
		return nil, 0, err
	}
	parts := strings.Split(string(decoded), ":")
	if len(parts) != 2 {
		return nil, 0, fmt.Errorf("invalid cursor")
	}
	ns, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return nil, 0, err
	}
	id, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return nil, 0, err
	}
	if ns <= 0 || id <= 0 {
		return nil, 0, fmt.Errorf("invalid cursor")
	}
	t := time.Unix(0, ns)
	return &t, id, nil
}

func encodeOrderCursor(at time.Time, id int64) string {
	return base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%d:%d", at.UnixNano(), id)))
}

func (h *Handler) listOrders(ctx context.Context, c *app.RequestContext) {
	uid, err := userIDFromContext(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
		return
	}
	const maxOrderPageSize = 100
	const maxOrderIDFilters = 50
	page := int32(1)
	pageSize := int32(20)
	if raw := strings.TrimSpace(c.Query("page")); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n <= 0 {
			c.JSON(http.StatusBadRequest, map[string]string{"error": "invalid page, expected integer >= 1"})
			return
		}
		page = int32(n)
	}
	if raw := strings.TrimSpace(c.Query("page_size")); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n <= 0 || n > maxOrderPageSize {
			c.JSON(http.StatusBadRequest, map[string]string{"error": "invalid page_size, expected integer 1-100"})
			return
		}
		pageSize = int32(n)
	}
	var fromTime *time.Time
	if raw := strings.TrimSpace(c.Query("from")); raw != "" {
		parsed, err := time.Parse(time.RFC3339, raw)
		if err != nil {
			c.JSON(http.StatusBadRequest, map[string]string{"error": "invalid from, expected RFC3339"})
			return
		}
		fromTime = &parsed
	}
	var toTime *time.Time
	if raw := strings.TrimSpace(c.Query("to")); raw != "" {
		parsed, err := time.Parse(time.RFC3339, raw)
		if err != nil {
			c.JSON(http.StatusBadRequest, map[string]string{"error": "invalid to, expected RFC3339"})
			return
		}
		toTime = &parsed
	}
	if fromTime != nil && toTime != nil && fromTime.After(*toTime) {
		c.JSON(http.StatusBadRequest, map[string]string{"error": "invalid time range: from after to"})
		return
	}
	var minAmount *float64
	if raw := strings.TrimSpace(c.Query("min_amount")); raw != "" {
		parsed, err := strconv.ParseFloat(raw, 64)
		if err != nil || parsed < 0 {
			c.JSON(http.StatusBadRequest, map[string]string{"error": "invalid min_amount, expected non-negative number"})
			return
		}
		minAmount = &parsed
	}
	var maxAmount *float64
	if raw := strings.TrimSpace(c.Query("max_amount")); raw != "" {
		parsed, err := strconv.ParseFloat(raw, 64)
		if err != nil || parsed < 0 {
			c.JSON(http.StatusBadRequest, map[string]string{"error": "invalid max_amount, expected non-negative number"})
			return
		}
		maxAmount = &parsed
	}
	if minAmount != nil && maxAmount != nil && *minAmount > *maxAmount {
		c.JSON(http.StatusBadRequest, map[string]string{"error": "invalid amount range: min_amount greater than max_amount"})
		return
	}
	var cursorAt *time.Time
	var cursorID int64
	if raw := strings.TrimSpace(c.Query("cursor")); raw != "" {
		parsedAt, parsedID, err := parseOrderCursor(raw)
		if err != nil {
			c.JSON(http.StatusBadRequest, map[string]string{"error": "invalid cursor"})
			return
		}
		cursorAt, cursorID = parsedAt, parsedID
	}
	rawStatus := strings.TrimSpace(c.Query("status"))
	status := ""
	var statuses []string
	if rawStatus != "" {
		parsed, err := parseOrderStatusList(rawStatus, allowedOrderStatuses)
		if err != nil {
			c.JSON(http.StatusBadRequest, map[string]string{"error": "invalid status, allowed: created, processing, shipped, completed, cancelled, failed"})
			return
		}
		if len(parsed) == 1 {
			status = parsed[0]
		} else {
			statuses = parsed
		}
	}
	var orderIDs []int64
	if raw := strings.TrimSpace(c.Query("order_ids")); raw != "" {
		ids, err := parseOrderIDList(raw, maxOrderIDFilters)
		if err != nil {
			msg := "invalid order_ids, expected comma/space-separated positive integers"
			if err == errTooManyOrderIDs {
				msg = fmt.Sprintf("too many order_ids, max %d", maxOrderIDFilters)
			}
			c.JSON(http.StatusBadRequest, map[string]string{"error": msg})
			return
		}
		orderIDs = ids
	}
	includeTotal := true
	if cursorAt != nil {
		includeTotal = false
	}
	if raw := strings.TrimSpace(c.Query("include_total")); raw != "" {
		parsed, ok := parseBoolQueryParam(raw)
		if !ok {
			c.JSON(http.StatusBadRequest, map[string]string{"error": "invalid include_total, expected true/false, yes/no, on/off, or 1/0"})
			return
		}
		if cursorAt != nil && parsed {
			c.JSON(http.StatusBadRequest, map[string]string{"error": "include_total not allowed when cursor is provided"})
			return
		}
		includeTotal = parsed
	}

	result, err := h.orderSvc.ListOrders(ctx, uid, service.OrderListFilter{Status: status, Statuses: statuses, OrderIDs: orderIDs, FromTime: fromTime, ToTime: toTime, MinAmount: minAmount, MaxAmount: maxAmount, Page: page, PageSize: pageSize, CursorAt: cursorAt, CursorID: cursorID, IncludeTotal: includeTotal})
	if err != nil {
		c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	nextCursor := ""
	if len(result.Items) > 0 {
		last := result.Items[len(result.Items)-1]
		nextCursor = encodeOrderCursor(last.CreatedAt, last.ID)
	}
	c.JSON(http.StatusOK, map[string]any{"items": result.Items, "page": result.Page, "page_size": result.PageSize, "total": result.Total, "next_cursor": nextCursor})
}

func (h *Handler) createReplayJob(ctx context.Context, c *app.RequestContext) {
	adminUID, err := adminUserIDFromContext(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
		return
	}
	if !h.requireAdminPermission(ctx, c, "replay_job:create") {
		return
	}
	commandID := commandIDFromHeader(c)
	traceID := traceIDFromHeader(c)
	if commandID == "" {
		c.JSON(http.StatusBadRequest, map[string]string{"error": "missing X-Command-Id"})
		return
	}
	if len(commandID) > 120 {
		c.JSON(http.StatusBadRequest, map[string]string{"error": "X-Command-Id too long"})
		return
	}
	locked, err := h.store.TryBeginAdminCommand(ctx, adminUID, "replay_job:create", commandID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if !locked {
		body, ok, err := h.waitAdminCommandResult(ctx, adminUID, "replay_job:create", commandID, traceID)
		if err != nil {
			c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		if ok {
			resp := map[string]any{}
			if json.Unmarshal(body, &resp) == nil {
				c.JSON(http.StatusOK, resp)
				return
			}
		}
		c.JSON(http.StatusConflict, map[string]string{"error": "command already processing"})
		return
	}
	var req struct {
		Limit int32  `json:"limit"`
		Topic string `json:"topic"`
	}
	if err := c.Bind(&req); err != nil {
		c.JSON(http.StatusBadRequest, map[string]string{"error": "invalid payload"})
		return
	}
	if req.Limit <= 0 {
		req.Limit = 50
	}
	replayCtx := repository.WithTraceMetadata(ctx, traceID, commandID)
	jobID, total, err := h.replayJobSvc.CreateJob(replayCtx, adminUID, strings.TrimSpace(req.Topic), req.Limit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	resp := map[string]any{"job_id": jobID, "queued_items": total}
	if b, err := json.Marshal(resp); err == nil {
		_ = h.store.SaveAdminCommandResult(ctx, adminUID, "replay_job:create", commandID, b)
	}
	logAdminCommandWait("info", "admin command executed", map[string]any{
		"action":        "replay_job:create",
		"command_id":    commandID,
		"trace_id":      traceID,
		"job_id":        jobID,
		"queued_items":  total,
		"actor_user_id": adminUID,
	})
	_ = h.store.InsertAuditLog(ctx, adminUID, "replay_job_create", "replay_job", fmt.Sprintf("%d", jobID), map[string]any{"topic": req.Topic, "limit": req.Limit, "total_items": total})
	c.JSON(http.StatusAccepted, resp)
}

func (h *Handler) getReplayJob(ctx context.Context, c *app.RequestContext) {
	_, err := adminUserIDFromContext(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
		return
	}
	if !h.requireAdminPermission(ctx, c, "replay_job:read") {
		return
	}
	jobID, err := strconv.ParseInt(c.Param("job_id"), 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, map[string]string{"error": "invalid job_id"})
		return
	}
	job, groups, err := h.replayJobSvc.GetJob(ctx, jobID)
	if err != nil {
		if err == repository.ErrNotFound {
			c.JSON(http.StatusNotFound, map[string]string{"error": "job not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, map[string]any{"job": job, "failed_groups": groups})
}

func (h *Handler) retryFailedReplayJob(ctx context.Context, c *app.RequestContext) {
	adminUID, err := adminUserIDFromContext(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
		return
	}
	if !h.requireAdminPermission(ctx, c, "replay_job:retry_failed") {
		return
	}
	commandID := commandIDFromHeader(c)
	traceID := traceIDFromHeader(c)
	if commandID == "" {
		c.JSON(http.StatusBadRequest, map[string]string{"error": "missing X-Command-Id"})
		return
	}
	locked, err := h.store.TryBeginAdminCommand(ctx, adminUID, "replay_job:retry_failed", commandID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if !locked {
		body, ok, err := h.waitAdminCommandResult(ctx, adminUID, "replay_job:retry_failed", commandID, traceID)
		if err != nil {
			c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		if ok {
			resp := map[string]any{}
			if json.Unmarshal(body, &resp) == nil {
				c.JSON(http.StatusOK, resp)
				return
			}
		}
		c.JSON(http.StatusConflict, map[string]string{"error": "command already processing"})
		return
	}
	jobID, err := strconv.ParseInt(c.Param("job_id"), 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, map[string]string{"error": "invalid job_id"})
		return
	}
	var req struct {
		ErrorGroup    string `json:"error_group"`
		Limit         int32  `json:"limit"`
		ResetAttempts *bool  `json:"reset_attempts"`
	}
	if err := c.Bind(&req); err != nil {
		req.Limit = 100
	}
	if req.Limit <= 0 {
		req.Limit = 100
	}
	resetAttempts := true
	if req.ResetAttempts != nil {
		resetAttempts = *req.ResetAttempts
	}
	replayCtx := repository.WithTraceMetadata(ctx, traceID, commandID)
	summary, err := h.replayJobSvc.RetryFailed(replayCtx, adminUID, jobID, strings.TrimSpace(req.ErrorGroup), req.Limit, resetAttempts)
	if err != nil {
		c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	resp := map[string]any{
		"job_id":                jobID,
		"retried":               summary.Retried,
		"reset_attempts":        resetAttempts,
		"attempts_before_total": summary.AttemptsBeforeTotal,
		"attempts_after_total":  summary.AttemptsAfterTotal,
		"error_groups_before":   summary.ErrorGroupsBefore,
		"error_groups_after":    summary.ErrorGroupsAfter,
	}
	if b, err := json.Marshal(resp); err == nil {
		_ = h.store.SaveAdminCommandResult(ctx, adminUID, "replay_job:retry_failed", commandID, b)
	}
	logAdminCommandWait("info", "admin command executed", map[string]any{
		"action":        "replay_job:retry_failed",
		"command_id":    commandID,
		"trace_id":      traceID,
		"job_id":        jobID,
		"retried":       summary.Retried,
		"actor_user_id": adminUID,
	})
	_ = h.store.InsertAuditLog(ctx, adminUID, "replay_job_retry_failed", "replay_job", fmt.Sprintf("%d", jobID), map[string]any{
		"error_group":           req.ErrorGroup,
		"limit":                 req.Limit,
		"retried":               summary.Retried,
		"reset_attempts":        resetAttempts,
		"attempts_before_total": summary.AttemptsBeforeTotal,
		"attempts_after_total":  summary.AttemptsAfterTotal,
		"error_groups_before":   summary.ErrorGroupsBefore,
		"error_groups_after":    summary.ErrorGroupsAfter,
	})
	c.JSON(http.StatusOK, resp)
}

func (h *Handler) searchReplayJobsByTrace(ctx context.Context, c *app.RequestContext) {
	if !h.requireAdminPermission(ctx, c, "replay_job:read") {
		return
	}
	traceID := strings.TrimSpace(c.Query("trace_id"))
	commandID := strings.TrimSpace(c.Query("command_id"))
	limit := int32(20)
	if raw := strings.TrimSpace(c.Query("limit")); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 {
			limit = int32(n)
		}
	}
	cursorID := int64(0)
	if raw := strings.TrimSpace(c.Query("cursor_id")); raw != "" {
		n, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || n < 0 {
			c.JSON(http.StatusBadRequest, map[string]string{"error": "invalid cursor_id"})
			return
		}
		cursorID = n
	}
	if traceID == "" && commandID == "" {
		c.JSON(http.StatusBadRequest, map[string]string{"error": "trace_id or command_id required"})
		return
	}
	items, err := h.store.SearchReplayJobsByCorrelation(ctx, traceID, commandID, limit, cursorID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	nextCursorID := int64(0)
	if len(items) > 0 {
		nextCursorID = items[len(items)-1].ID
	}
	c.JSON(http.StatusOK, map[string]any{"items": items, "count": len(items), "next_cursor_id": nextCursorID})
}

func (h *Handler) searchOutboxEventsByTrace(ctx context.Context, c *app.RequestContext) {
	if !h.requireAdminPermission(ctx, c, "replay_job:read") {
		return
	}
	traceID := strings.TrimSpace(c.Query("trace_id"))
	commandID := strings.TrimSpace(c.Query("command_id"))
	limit := int32(20)
	if raw := strings.TrimSpace(c.Query("limit")); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 {
			limit = int32(n)
		}
	}
	cursorID := int64(0)
	if raw := strings.TrimSpace(c.Query("cursor_id")); raw != "" {
		n, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || n < 0 {
			c.JSON(http.StatusBadRequest, map[string]string{"error": "invalid cursor_id"})
			return
		}
		cursorID = n
	}
	if traceID == "" && commandID == "" {
		c.JSON(http.StatusBadRequest, map[string]string{"error": "trace_id or command_id required"})
		return
	}
	items, err := h.store.SearchOutboxEventsByCorrelation(ctx, traceID, commandID, limit, cursorID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	nextCursorID := int64(0)
	if len(items) > 0 {
		nextCursorID = items[len(items)-1].ID
	}
	c.JSON(http.StatusOK, map[string]any{"items": items, "count": len(items), "next_cursor_id": nextCursorID})
}

func (h *Handler) listAuditLogs(ctx context.Context, c *app.RequestContext) {
	if !h.requireAdminPermission(ctx, c, "audit:read") {
		return
	}
	action := strings.TrimSpace(c.Query("action"))
	search := strings.TrimSpace(c.Query("q"))
	limit := int32(50)
	if raw := strings.TrimSpace(c.Query("limit")); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 {
			limit = int32(n)
		}
	}
	cursorID := int64(0)
	if raw := strings.TrimSpace(c.Query("cursor_id")); raw != "" {
		if n, err := strconv.ParseInt(raw, 10, 64); err == nil && n > 0 {
			cursorID = n
		}
	}
	items, err := h.authSvc.ListAuditLogs(ctx, action, search, limit, cursorID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	nextCursorID := int64(0)
	if len(items) > 0 {
		nextCursorID = items[len(items)-1].ID
	}
	c.JSON(http.StatusOK, map[string]any{"items": items, "next_cursor_id": nextCursorID})
}

func (h *Handler) exportAuditLogs(ctx context.Context, c *app.RequestContext) {
	if !h.requireAdminPermission(ctx, c, "audit:read") {
		return
	}
	action := strings.TrimSpace(c.Query("action"))
	search := strings.TrimSpace(c.Query("q"))
	limit := int32(1000)
	items, err := h.authSvc.ListAuditLogs(ctx, action, search, limit, 0)
	if err != nil {
		c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	c.Header("Content-Type", "text/csv; charset=utf-8")
	c.Header("Content-Disposition", "attachment; filename=audit_logs.csv")
	var b strings.Builder
	writer := csv.NewWriter(&b)
	_ = writer.Write([]string{"id", "actor_user_id", "action", "target_type", "target_id", "payload", "created_at"})
	for _, it := range items {
		_ = writer.Write([]string{
			strconv.FormatInt(it.ID, 10),
			strconv.FormatInt(it.ActorUser, 10),
			it.Action,
			it.TargetType,
			it.TargetID,
			string(it.Payload),
			it.CreatedAt.Format(time.RFC3339),
		})
	}
	writer.Flush()
	c.String(http.StatusOK, b.String())
}
