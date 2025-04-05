package servers

import (
	"fmt"
	healthProto "github.com/gisit-triggis/gisit-proto/gen/go/health/v1"
	positionProto "github.com/gisit-triggis/gisit-proto/gen/go/position/v1"
	"github.com/gisit-triggis/gisit-realtime-backend/internal/app/handlers"
	"github.com/gisit-triggis/gisit-realtime-backend/internal/app/ws"
	"github.com/scylladb/gocqlx/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"net"
	"os"
)

func InitGrpcServer(client *gocqlx.Session, logger *zap.Logger, wsHub *ws.WsHub) (*grpc.Server, func()) {
	healthHandler, positionHandler, cleanup := handlers.InitHandlers(client, logger, wsHub)

	grpcServer := grpc.NewServer()
	healthProto.RegisterHealthServer(grpcServer, healthHandler)
	positionProto.RegisterPositionServiceServer(grpcServer, positionHandler)

	healthServer := health.NewServer()
	healthpb.RegisterHealthServer(grpcServer, healthServer)

	healthServer.SetServingStatus("health.v1.Health", healthpb.HealthCheckResponse_SERVING)
	healthServer.SetServingStatus("position.v1.PositionService", healthpb.HealthCheckResponse_SERVING)

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
