package service

import "testing"

func TestGenerateTokenPrefix(t *testing.T) {
	tok, err := generateToken()
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(tok) < 8 || tok[:4] != "tok_" {
		t.Fatalf("bad token: %s", tok)
	}
}
