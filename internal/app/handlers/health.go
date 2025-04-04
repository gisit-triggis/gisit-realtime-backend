package handlers

import (
	"context"
	proto "github.com/gisit-triggis/gisit-proto/gen/go/health/v1"
	"go.uber.org/zap"
)

type HealthHandler struct {
	proto.UnimplementedHealthServer
	logger *zap.Logger
}

func NewHealthHandler(logger *zap.Logger) *HealthHandler {
	return &HealthHandler{
		logger: logger,
	}
}

func (h *HealthHandler) Check(ctx context.Context, req *proto.HealthCheckRequest) (*proto.HealthCheckResponse, error) {
	return &proto.HealthCheckResponse{
		Status: 1,
	}, nil
}
