package domain

import "time"

type User struct {
	ID           int64  `json:"id"`
	Email        string `json:"email"`
	PasswordHash string `json:"-"`
	Name         string `json:"name"`
	Role         string `json:"role"`
}

type Product struct {
	ID    int64   `json:"id"`
	Title string  `json:"title"`
	Price float64 `json:"price"`
	Stock int32   `json:"stock"`
}

type CartItem struct {
	ProductID int64   `json:"product_id"`
	Title     string  `json:"title"`
	Price     float64 `json:"price"`
	Quantity  int32   `json:"quantity"`
}

type Order struct {
	ID        int64     `json:"order_id"`
	UserID    int64     `json:"user_id"`
	Address   string    `json:"address"`
	Amount    float64   `json:"amount"`
	Status    string    `json:"status"`
	ItemCount int32     `json:"item_count"`
	CreatedAt time.Time `json:"created_at"`
}

type OrderItem struct {
	ProductID int64   `json:"product_id"`
	Title     string  `json:"title"`
	Price     float64 `json:"price"`
	Quantity  int32   `json:"quantity"`
	Subtotal  float64 `json:"subtotal"`
}

type OrderDetail struct {
	Order
	Items                   []OrderItem `json:"items"`
	IdempotencyKey          string      `json:"idempotency_key,omitempty"`
	IdempotencyCreatedAt    *time.Time  `json:"idempotency_created_at,omitempty"`
	IdempotencyLastReplayAt *time.Time  `json:"idempotency_last_replay_at,omitempty"`
}
