package http

import (
	"testing"

	"github.com/cloudwego/hertz/pkg/app/server"
)

func TestRegisterRoutesDoesNotPanic(t *testing.T) {
	h := server.Default()
	RegisterRoutes(h, &Handler{})
}
