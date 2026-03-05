package http

import (
	"context"
	"testing"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/route/param"
)

func TestHandlerErrorPathsWithoutServiceDependencies(t *testing.T) {
	h := &Handler{}

	{
		c := app.NewContext(0)
		h.getCart(context.Background(), c)
		assertStatus(t, c, 401)
	}
	{
		c := app.NewContext(0)
		h.addCartItem(context.Background(), c)
		assertStatus(t, c, 401)
	}
	{
		c := app.NewContext(0)
		c.Set("user_id", int64(1))
		c.Request.SetBodyString("{")
		h.addCartItem(context.Background(), c)
		assertStatus(t, c, 400)
	}
	{
		c := app.NewContext(0)
		c.Set("user_id", int64(1))
		c.Request.SetBodyString(`{"product_id":1,"quantity":0}`)
		h.addCartItem(context.Background(), c)
		assertStatus(t, c, 400)
	}
	{
		c := app.NewContext(0)
		c.Set("user_id", int64(1))
		c.Params = param.Params{{Key: "product_id", Value: "bad"}}
		h.removeCartItem(context.Background(), c)
		assertStatus(t, c, 400)
	}
	{
		c := app.NewContext(0)
		h.placeOrder(context.Background(), c)
		assertStatus(t, c, 401)
	}
	{
		c := app.NewContext(0)
		c.Set("user_id", int64(1))
		h.placeOrder(context.Background(), c)
		assertStatus(t, c, 400)
	}
	{
		c := app.NewContext(0)
		c.Set("user_id", int64(1))
		c.Request.Header.Set("Idempotency-Key", "idem-1")
		c.Request.SetBodyString("{")
		h.placeOrder(context.Background(), c)
		assertStatus(t, c, 400)
	}
	{
		c := app.NewContext(0)
		c.Set("user_id", int64(1))
		c.Request.Header.Set("Idempotency-Key", "idem-2")
		c.Request.SetBodyString(`{"address":"   "}`)
		h.placeOrder(context.Background(), c)
		assertStatus(t, c, 400)
	}
	{
		c := app.NewContext(0)
		c.Set("user_id", int64(1))
		c.Params = param.Params{{Key: "order_id", Value: "bad"}}
		h.getOrder(context.Background(), c)
		assertStatus(t, c, 400)
	}
	{
		c := app.NewContext(0)
		c.Set("user_id", int64(1))
		c.Request.SetQueryString("from=bad")
		h.listOrders(context.Background(), c)
		assertStatus(t, c, 400)
	}
	{
		c := app.NewContext(0)
		c.Set("user_id", int64(1))
		c.Request.SetQueryString("to=bad")
		h.listOrders(context.Background(), c)
		assertStatus(t, c, 400)
	}
	{
		c := app.NewContext(0)
		c.Set("user_id", int64(1))
		c.Request.SetQueryString("from=2026-03-05T00:00:00Z&to=2026-03-04T00:00:00Z")
		h.listOrders(context.Background(), c)
		assertStatus(t, c, 400)
	}
	{
		c := app.NewContext(0)
		c.Set("user_id", int64(1))
		c.Request.SetQueryString("min_amount=bad")
		h.listOrders(context.Background(), c)
		assertStatus(t, c, 400)
	}
	{
		c := app.NewContext(0)
		c.Set("user_id", int64(1))
		c.Request.SetQueryString("min_amount=-1")
		h.listOrders(context.Background(), c)
		assertStatus(t, c, 400)
	}
	{
		c := app.NewContext(0)
		c.Set("user_id", int64(1))
		c.Request.SetQueryString("max_amount=oops")
		h.listOrders(context.Background(), c)
		assertStatus(t, c, 400)
	}
	{
		c := app.NewContext(0)
		c.Set("user_id", int64(1))
		c.Request.SetQueryString("min_amount=100&max_amount=10")
		h.listOrders(context.Background(), c)
		assertStatus(t, c, 400)
	}
	{
		c := app.NewContext(0)
		c.Set("user_id", int64(1))
		c.Request.SetQueryString("cursor=bad")
		h.listOrders(context.Background(), c)
		assertStatus(t, c, 400)
	}
	{
		c := app.NewContext(0)
		c.Set("user_id", int64(1))
		c.Request.SetQueryString("status=unknown")
		h.listOrders(context.Background(), c)
		assertStatus(t, c, 400)
	}
	{
		c := app.NewContext(0)
		c.Set("user_id", int64(1))
		c.Request.SetQueryString("status=created,unknown")
		h.listOrders(context.Background(), c)
		assertStatus(t, c, 400)
	}
	{
		c := app.NewContext(0)
		c.Set("user_id", int64(1))
		c.Request.SetQueryString("order_ids=1,bad")
		h.listOrders(context.Background(), c)
		assertStatus(t, c, 400)
	}
	{
		c := app.NewContext(0)
		c.Set("user_id", int64(1))
		c.Request.SetQueryString("include_total=maybe")
		h.listOrders(context.Background(), c)
		assertStatus(t, c, 400)
	}
	{
		c := app.NewContext(0)
		c.Set("user_id", int64(1))
		c.Request.SetQueryString("cursor=MDox&include_total=true")
		h.listOrders(context.Background(), c)
		assertStatus(t, c, 400)
	}
	{
		c := app.NewContext(0)
		c.Set("user_id", int64(1))
		c.Request.SetQueryString("page=0")
		h.listOrders(context.Background(), c)
		assertStatus(t, c, 400)
	}
	{
		c := app.NewContext(0)
		c.Set("user_id", int64(1))
		c.Request.SetQueryString("page=oops")
		h.listOrders(context.Background(), c)
		assertStatus(t, c, 400)
	}
	{
		c := app.NewContext(0)
		c.Set("user_id", int64(1))
		c.Request.SetQueryString("page_size=101")
		h.listOrders(context.Background(), c)
		assertStatus(t, c, 400)
	}
	{
		c := app.NewContext(0)
		h.getReplayJob(context.Background(), c)
		assertStatus(t, c, 401)
	}
	{
		c := app.NewContext(0)
		h.retryFailedReplayJob(context.Background(), c)
		assertStatus(t, c, 401)
	}
}

func assertStatus(t *testing.T, c *app.RequestContext, want int) {
	t.Helper()
	if got := c.Response.StatusCode(); got != want {
		t.Fatalf("status mismatch got=%d want=%d", got, want)
	}
}
