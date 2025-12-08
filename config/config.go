package config

// Решил остановиться на минимальном варианте без .env и сторонних библиотек
type Config struct {
	MaxMessages int32
	MaxRetries  int
}

func NewConfig() *Config {
	return &Config{
		MaxMessages: 200,
		MaxRetries:  3,
	}
}
