package prelude

import (
	"github.com/redis/go-redis/v9"
	"os"
)

func InitRedisClient() *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:     os.Getenv("REDIS_REALTIME_HOST"),
		Password: "",
		DB:       0,
	})
}
