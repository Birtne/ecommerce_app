package config

import "os"

type Config struct {
	HTTPAddr    string
	PostgresDSN string
	RedisAddr   string
	NATSURL     string
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func Load() Config {
	return Config{
		HTTPAddr:    getEnv("HTTP_ADDR", ":8888"),
		PostgresDSN: getEnv("POSTGRES_DSN", "postgres://ecom:ecom@localhost:5432/ecommerce?sslmode=disable"),
		RedisAddr:   getEnv("REDIS_ADDR", "localhost:6379"),
		NATSURL:     getEnv("NATS_URL", "nats://localhost:4222"),
	}
}
