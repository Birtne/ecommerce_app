package service

import (
	"os"
	"time"
)

type Config struct {
	PostgresDSN     string
	RedisAddr       string
	NATSURL         string
	ShutdownTimeout time.Duration
}

func LoadConfig() Config {
	return Config{
		PostgresDSN:     getenv("POSTGRES_DSN", "postgres://ecommerce:ecommerce@localhost:5432/ecommerce?sslmode=disable"),
		RedisAddr:       getenv("REDIS_ADDR", "localhost:6379"),
		NATSURL:         getenv("NATS_URL", "nats://localhost:4222"),
		ShutdownTimeout: 5 * time.Second,
	}
}

func getenv(key, fallback string) string {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	return v
}
