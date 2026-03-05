package service

import (
	"errors"
	"strings"
	"testing"
)

func TestFormatErr(t *testing.T) {
	err := FormatErr("ctx", errors.New("root"))
	if err == nil || !strings.Contains(err.Error(), "ctx") {
		t.Fatalf("expected wrapped error")
	}
}
