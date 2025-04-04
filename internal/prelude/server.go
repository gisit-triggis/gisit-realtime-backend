package prelude

import (
	"context"
	"github.com/gisit-triggis/gisit-realtime-backend/internal/app/ws"
	"github.com/gisit-triggis/gisit-realtime-backend/internal/prelude/servers"
	"github.com/redis/go-redis/v9"
	"github.com/scylladb/gocqlx/v3"
	"go.uber.org/zap"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func InitServer(client *gocqlx.Session, logger *zap.Logger, redis *redis.Client) {
	defer logger.Info("Servers exited properly")
	defer client.Close()

	wsHub := ws.NewWsHub(os.Getenv("NODE_ID"), redis)

	grpcServer, cleanup := servers.InitGrpcServer(client, logger)
	defer cleanup()
	defer grpcServer.GracefulStop()

	ginServer, cleanup := servers.InitGinServer(logger, wsHub)
	defer cleanup()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := ginServer.Shutdown(ctx); err != nil {
			logger.Fatal("HTTP server forced to shutdown", zap.Error(err))
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.Info("Shutting down servers...")
}
