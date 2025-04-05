package handlers

import (
	"context"
	"fmt"
	"io"
	"time"

	proto "github.com/gisit-triggis/gisit-proto/gen/go/position/v1"
	"github.com/gisit-triggis/gisit-realtime-backend/internal/app/ws"
	"github.com/scylladb/gocqlx/v3"
	"github.com/scylladb/gocqlx/v3/table"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type UserPosition struct {
	UserID      string    `db:"user_id"`
	Latitude    float64   `db:"lat"`
	Longitude   float64   `db:"lon"`
	Speed       float64   `db:"speed"`
	Status      string    `db:"status"`
	LastUpdated time.Time `db:"last_updated"`
}

var userPositionTable = table.New(table.Metadata{
	Name: "user_live_state",
	Columns: []string{
		"user_id",
		"lat",
		"lon",
		"speed",
		"status",
		"last_updated",
	},
	PartKey: []string{"user_id"},
	SortKey: nil,
})

type PositionHandler struct {
	proto.UnimplementedPositionServiceServer
	session *gocqlx.Session
	logger  *zap.Logger
	wsHub   *ws.WsHub
}

func NewPositionHandler(session *gocqlx.Session, logger *zap.Logger, wsHub *ws.WsHub) *PositionHandler {
	return &PositionHandler{
		session: session,
		logger:  logger,
		wsHub:   wsHub,
	}
}

func (h *PositionHandler) UpdatePosition(ctx context.Context, req *proto.PositionUpdate) (*proto.PositionResponse, error) {
	if req.UserId == "" {
		return nil, status.Error(codes.InvalidArgument, "user_id is required")
	}

	if err := h.savePosition(ctx, req); err != nil {
		h.logger.Error("Failed to save position", zap.Error(err))
		return nil, status.Error(codes.Internal, "failed to save position")
	}

	if err := h.broadcastPositionUpdate(ctx, req); err != nil {
		h.logger.Error("Failed to broadcast position update", zap.Error(err))
	}

	return &proto.PositionResponse{
		Success: true,
		Message: "Position updated successfully",
	}, nil
}

func (h *PositionHandler) StreamPositionUpdates(stream proto.PositionService_StreamPositionUpdatesServer) error {
	ctx := stream.Context()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			update, err := stream.Recv()
			if err == io.EOF {
				return nil
			}
			if err != nil {
				h.logger.Error("Error receiving position update", zap.Error(err))
				return err
			}

			if err := h.savePosition(ctx, update); err != nil {
				h.logger.Error("Failed to save position", zap.Error(err))
				continue
			}

			if err := h.broadcastPositionUpdate(ctx, update); err != nil {
				h.logger.Error("Failed to broadcast position update", zap.Error(err))
			}

			if err := stream.SendMsg(&proto.PositionResponse{
				Success: true,
				Message: "Position updated successfully",
			}); err != nil {
				h.logger.Error("Failed to send acknowledgment", zap.Error(err))
				return err
			}
		}
	}
}

func (h *PositionHandler) GetAllPositions(ctx context.Context, req *proto.GetAllPositionsRequest) (*proto.GetAllPositionsResponse, error) {
	positions, err := h.getAllPositions(ctx)
	if err != nil {
		h.logger.Error("Failed to retrieve positions", zap.Error(err))
		return nil, status.Error(codes.Internal, "failed to retrieve positions")
	}

	return &proto.GetAllPositionsResponse{
		Positions: positions,
	}, nil
}

type PositionHistory struct {
	UserID    string    `db:"user_id"`
	Timestamp time.Time `db:"timestamp"`
	Latitude  float64   `db:"lat"`
	Longitude float64   `db:"lon"`
	Speed     float64   `db:"speed"`
	Status    string    `db:"status"`
}

var positionHistoryTable = table.New(table.Metadata{
	Name: "position_history",
	Columns: []string{
		"user_id",
		"timestamp",
		"lat",
		"lon",
		"speed",
		"status",
	},
	PartKey: []string{"user_id"},
	SortKey: []string{"timestamp"},
})

func (h *PositionHandler) savePosition(ctx context.Context, update *proto.PositionUpdate) error {
	timestamp, err := time.Parse(time.RFC3339, update.Timestamp)
	if err != nil {
		timestamp = time.Now()
	}

	position := UserPosition{
		UserID:      update.UserId,
		Latitude:    update.Latitude,
		Longitude:   update.Longitude,
		Speed:       update.Speed,
		Status:      update.Status,
		LastUpdated: timestamp,
	}

	history := PositionHistory{
		UserID:    update.UserId,
		Timestamp: timestamp,
		Latitude:  update.Latitude,
		Longitude: update.Longitude,
		Speed:     update.Speed,
		Status:    update.Status,
	}

	stmt, names := userPositionTable.Insert()
	q := h.session.Query(stmt, names).BindStruct(position)
	if err := q.ExecRelease(); err != nil {
		return fmt.Errorf("failed to save current position: %w", err)
	}

	stmt, names = positionHistoryTable.Insert()
	q = h.session.Query(stmt, names).BindStruct(history)
	if err := q.ExecRelease(); err != nil {
		h.logger.Error("Failed to save position history", zap.Error(err))
	}

	return nil
}

func (h *PositionHandler) getAllPositions(ctx context.Context) ([]*proto.PositionUpdate, error) {
	var positions []UserPosition
	stmt, names := userPositionTable.Select()
	q := h.session.Query(stmt, names)
	if err := q.SelectRelease(&positions); err != nil {
		return nil, err
	}

	result := make([]*proto.PositionUpdate, 0, len(positions))
	for _, p := range positions {
		result = append(result, &proto.PositionUpdate{
			UserId:    p.UserID,
			Latitude:  p.Latitude,
			Longitude: p.Longitude,
			Speed:     p.Speed,
			Status:    p.Status,
			Timestamp: p.LastUpdated.Format(time.RFC3339),
		})
	}

	return result, nil
}

func (h *PositionHandler) broadcastPositionUpdate(ctx context.Context, update *proto.PositionUpdate) error {
	messageData, err := ws.ConvertPositionUpdateToJSON(update)
	if err != nil {
		return err
	}

	return h.wsHub.BroadcastMessage(ctx, messageData)
}
