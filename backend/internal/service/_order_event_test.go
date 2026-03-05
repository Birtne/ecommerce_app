package service

import "testing"

func TestOrderCreatedEventFields(t *testing.T) {
	evt := OrderCreatedEvent{OrderID: 101, UserID: 7, Total: 88.8, Address: "Road 1"}
	if evt.OrderID != 101 || evt.UserID != 7 || evt.Total != 88.8 || evt.Address != "Road 1" {
		t.Fatalf("event fields mismatch")
	}
}
