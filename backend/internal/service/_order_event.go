package service

import (
	"encoding/json"
	"fmt"

	"github.com/nats-io/nats.go"
)

type OrderCreatedEvent struct {
	OrderID int64   `json:"order_id"`
	UserID  int64   `json:"user_id"`
	Total   float64 `json:"total"`
	Address string  `json:"address"`
}

type EventPublisher interface {
	PublishOrderCreated(evt OrderCreatedEvent) error
	Close()
}

type NATSPublisher struct {
	nc *nats.Conn
}

func NewNATSPublisher(url string) *NATSPublisher {
	nc, err := nats.Connect(url)
	if err != nil {
		return &NATSPublisher{}
	}
	return &NATSPublisher{nc: nc}
}

func (n *NATSPublisher) PublishOrderCreated(evt OrderCreatedEvent) error {
	if n.nc == nil {
		return nil
	}
	payload, err := json.Marshal(evt)
	if err != nil {
		return fmt.Errorf("marshal order event: %w", err)
	}
	if err := n.nc.Publish("orders.created", payload); err != nil {
		return fmt.Errorf("publish nats event: %w", err)
	}
	return nil
}

func (n *NATSPublisher) Close() {
	if n.nc != nil {
		n.nc.Close()
	}
}
