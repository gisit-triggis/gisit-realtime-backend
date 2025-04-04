package main

import "github.com/gisit-triggis/gisit-realtime-backend/internal/prelude"

func main() {
	logger := prelude.InitLogger()
	client := prelude.InitClient(logger)
	redis := prelude.InitRedisClient()

	prelude.InitServer(client, logger, redis)
}
