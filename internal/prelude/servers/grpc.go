package servers

import (
	"fmt"
	proto "github.com/gisit-triggis/gisit-proto/gen/go/health/v1"
	"github.com/gisit-triggis/gisit-realtime-backend/internal/app/handlers"
	"github.com/scylladb/gocqlx/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"net"
	"os"
)

func InitGrpcServer(client *gocqlx.Session, logger *zap.Logger) (*grpc.Server, func()) {
	healthHandler, cleanup := handlers.InitHandlers(client, logger)

	grpcServer := grpc.NewServer()
	proto.RegisterHealthServer(grpcServer, healthHandler)

	healthServer := health.NewServer()
	healthpb.RegisterHealthServer(grpcServer, healthServer)

	grpcAddr := fmt.Sprintf(":%s", os.Getenv("GRPC_PORT"))
	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		logger.Fatal("Failed to listen", zap.Error(err))
	}

	go func() {
		logger.Info("Starting gRPC server", zap.String("addr", grpcAddr))
		if err := grpcServer.Serve(lis); err != nil {
			logger.Fatal("Failed to serve gRPC", zap.Error(err))
		}
	}()

	return grpcServer, cleanup
}
