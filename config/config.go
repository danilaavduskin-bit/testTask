package config

import (
	"github.com/joho/godotenv"
	"log"
	"os"
	"strconv"
)

type Config struct {
	MaxMessages int32
	MaxRetries  int
}

func EnvInit() *Config {
	if err := godotenv.Load("./.env"); err != nil {
		log.Fatal("Error loading .env file")
		return nil
	}

	cfg := &Config{
		MaxMessages: 200, // значения по умолчанию
		MaxRetries:  3,
	}

	if val := os.Getenv("MAX_MSG"); val != "" {
		if n, err := strconv.Atoi(val); err == nil && n > 0 {
			cfg.MaxMessages = int32(n)
		}
	}

	if val := os.Getenv("MAX_RETRY"); val != "" {
		if n, err := strconv.Atoi(val); err == nil && n >= 0 {
			cfg.MaxRetries = n
		}
	}

	return cfg
}
