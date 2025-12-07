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

	maxM, err := strconv.Atoi(os.Getenv("MAX_MSG"))
	if err != nil || maxM <= 0 {
		maxM = 200
	}
	maxR, err := strconv.Atoi(os.Getenv("MAX_RETRY"))
	if err != nil || maxR < 0 {
		maxM = 3
	}

	return &Config{
		MaxMessages: int32(maxM),
		MaxRetries:  maxR,
	}
}
