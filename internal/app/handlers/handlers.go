package handlers

import (
	"github.com/scylladb/gocqlx/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"os"
)

func InitHandlers(session *gocqlx.Session, logger *zap.Logger) (*HealthHandler, func()) {
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

	return healthHandler, cleanup
}

func initGRPCClient(addr string) (*grpc.ClientConn, error) {
	return grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
}
