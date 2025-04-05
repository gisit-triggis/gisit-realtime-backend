package handlers

import (
	"github.com/gisit-triggis/gisit-realtime-backend/internal/app/ws"
	"github.com/scylladb/gocqlx/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"os"
)

func InitHandlers(session *gocqlx.Session, logger *zap.Logger, wsHub *ws.WsHub) (*HealthHandler, *PositionHandler, func()) {
	healthConn, err := initGRPCClient(os.Getenv("HEALTH_SERVICE_ADDR"))
	if err != nil {
		logger.Fatal("Failed to connect to catalog service", zap.Error(err))
	}

	cleanup := func() {
		if err := healthConn.Close(); err != nil {
			logger.Error("Failed to close auth service connection", zap.Error(err))
		}
	}

	healthHandler := NewHealthHandler(logger)
	positionHandler := NewPositionHandler(session, logger, wsHub)

	return healthHandler, positionHandler, cleanup
}

func initGRPCClient(addr string) (*grpc.ClientConn, error) {
	return grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
}
