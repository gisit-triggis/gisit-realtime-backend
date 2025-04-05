package handlers

import (
	"context"
	"fmt"
	"github.com/gocql/gocql"
	"github.com/google/uuid"
	"github.com/scylladb/gocqlx/v3/qb"
	"io"
	"os"
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

var userPositionMetadata = table.Metadata{
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
}

var userPositionTable = table.New(userPositionMetadata)

type PositionHistory struct {
	UserID    string    `db:"user_id"`
	Timestamp time.Time `db:"timestamp"`
	Latitude  float64   `db:"lat"`
	Longitude float64   `db:"lon"`
	Speed     float64   `db:"speed"`
	Status    string    `db:"status"`
}

var positionHistoryMetadata = table.Metadata{ // Переименовано для ясности
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
}
var positionHistoryTable = table.New(positionHistoryMetadata)

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

	h.logger.Info("Received position update request",
		zap.String("userId", req.UserId),
		zap.Float64("lat", req.Latitude),
		zap.Float64("lon", req.Longitude),
		zap.Float64("speed", req.Speed),
		zap.String("status", req.Status),
		zap.String("timestamp", req.Timestamp))

	if req.Timestamp == "" {
		req.Timestamp = time.Now().Format(time.RFC3339)
	} else {
		_, err := time.Parse(time.RFC3339, req.Timestamp)
		if err != nil {
			h.logger.Warn("Invalid timestamp format received",
				zap.String("userId", req.UserId),
				zap.String("timestamp", req.Timestamp),
				zap.Error(err))
			req.Timestamp = time.Now().Format(time.RFC3339)
		}
	}

	if err := h.savePosition(ctx, req); err != nil {
		h.logger.Error("Failed to save position",
			zap.String("userId", req.UserId),
			zap.Error(err))
		return nil, status.Error(codes.Internal, "failed to save position")
	} else {
		h.logger.Info("Position saved successfully", zap.String("userId", req.UserId))
	}

	if err := h.broadcastPositionUpdate(ctx, req); err != nil {
		h.logger.Error("Failed to broadcast position update",
			zap.String("userId", req.UserId),
			zap.Error(err))
	} else {
		h.logger.Info("Position update broadcast successfully", zap.String("userId", req.UserId))
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

			if update.UserId == "" {
				h.logger.Warn("Received position update with empty user_id, skipping")
				continue
			}

			if update.Timestamp == "" {
				update.Timestamp = time.Now().Format(time.RFC3339)
			} else {
				_, err := time.Parse(time.RFC3339, update.Timestamp)
				if err != nil {
					h.logger.Warn("Invalid timestamp format received in stream",
						zap.String("userId", update.UserId),
						zap.String("timestamp", update.Timestamp),
						zap.Error(err))
					update.Timestamp = time.Now().Format(time.RFC3339)
				}
			}

			if err := h.savePosition(ctx, update); err != nil {
				h.logger.Error("Failed to save position", zap.Error(err))
				if err := stream.SendMsg(&proto.PositionResponse{
					Success: false,
					Message: "Failed to save position: " + err.Error(),
				}); err != nil {
					h.logger.Error("Failed to send error response", zap.Error(err))
					return err
				}
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
		if err == gocql.ErrNotFound {
			h.logger.Info("GetAllPositions completed: No positions found.")
			return &proto.GetAllPositionsResponse{
				Positions: []*proto.PositionUpdate{},
			}, nil
		}
		h.logger.Error("Failed to retrieve positions from getAllPositions", zap.Error(err))
		return nil, status.Error(codes.Internal, "failed to retrieve positions")
	}

	return &proto.GetAllPositionsResponse{
		Positions: positions,
	}, nil
}

func (h *PositionHandler) savePosition(ctx context.Context, update *proto.PositionUpdate) error {
	timestamp, err := time.Parse(time.RFC3339, update.Timestamp)
	if err != nil {
		timestamp = time.Now()
		h.logger.Warn("Using current time instead of invalid timestamp",
			zap.String("userId", update.UserId),
			zap.String("originalTimestamp", update.Timestamp),
			zap.Time("newTimestamp", timestamp))
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

	h.logger.Debug("Preparing to save position",
		zap.String("userId", update.UserId),
		zap.Any("position", position),
		zap.Any("history", history))

	keyspace := h.getKeyspaceName()

	stmtLive, namesLive := qb.Insert(keyspace + "." + userPositionTable.Name()).
		Columns(userPositionMetadata.Columns...).
		ToCql()
	qLive := h.session.ContextQuery(ctx, stmtLive, namesLive).BindStruct(position)

	h.logger.Debug("Executing query for current position",
		zap.String("statement", stmtLive),
		zap.Strings("names", namesLive))

	if err := qLive.ExecRelease(); err != nil {
		return fmt.Errorf("failed to save current position: %w", err)
	}
	h.logger.Debug("Current position saved successfully", zap.String("userId", update.UserId))

	stmtHistory, namesHistory := qb.Insert(keyspace + "." + positionHistoryTable.Name()).
		Columns(positionHistoryMetadata.Columns...).
		ToCql()
	qHistory := h.session.ContextQuery(ctx, stmtHistory, namesHistory).BindStruct(history)
	h.logger.Debug("Executing query for position history",
		zap.String("statement", stmtHistory),
		zap.Strings("names", namesHistory))

	if err := qHistory.ExecRelease(); err != nil {
		h.logger.Error("Failed to save position history",
			zap.String("userId", update.UserId),
			zap.Error(err))
	} else {
		h.logger.Debug("Position history saved successfully", zap.String("userId", update.UserId))
	}

	return nil
}

func (h *PositionHandler) getKeyspaceName() string {
	keyspace := os.Getenv("SCYLLA_KEYSPACE")
	if keyspace == "" {
		keyspace = "realtime"
	}
	return keyspace
}

func (h *PositionHandler) getAllPositions(ctx context.Context) ([]*proto.PositionUpdate, error) {
	h.logger.Info("Getting all positions from database")

	var positions []UserPosition
	keyspace := h.getKeyspaceName()
	fullTableName := keyspace + "." + userPositionTable.Name()

	stmt, names := qb.Select(fullTableName).
		Columns(userPositionMetadata.Columns...).
		ToCql()

	q := h.session.ContextQuery(ctx, stmt, names)

	err := q.SelectRelease(&positions)
	if err != nil {
		if err == gocql.ErrNotFound {
			h.logger.Info("No live positions found in database.")
		} else {
			h.logger.Error("Error querying positions", zap.String("statement", stmt), zap.Error(err))
		}
		return []*proto.PositionUpdate{}, err
	}

	h.logger.Info("Retrieved positions from database", zap.Int("count", len(positions)))

	result := make([]*proto.PositionUpdate, len(positions))
	for i, p := range positions {
		result[i] = &proto.PositionUpdate{
			UserId:    p.UserID,
			Latitude:  p.Latitude,
			Longitude: p.Longitude,
			Speed:     p.Speed,
			Status:    p.Status,
			Timestamp: p.LastUpdated.Format(time.RFC3339),
		}
	}

	if len(result) == 0 && os.Getenv("DEBUG") == "true" && os.Getenv("INSERT_TEST_POSITION") == "true" {
		h.logger.Info("No positions found, adding test position for debugging")
		testUser := "test-user-" + uuid.New().String()[:8]
		testPosProto := &proto.PositionUpdate{
			UserId:    testUser,
			Latitude:  55.751244,
			Longitude: 37.618423,
			Speed:     60.0,
			Status:    "ACTIVE",
			Timestamp: time.Now().Format(time.RFC3339),
		}
		if saveErr := h.savePosition(ctx, testPosProto); saveErr != nil {
			h.logger.Error("Failed to save test position", zap.Error(saveErr))
			return []*proto.PositionUpdate{}, nil
		}
		h.logger.Info("Test position saved successfully", zap.String("userId", testUser))
		return []*proto.PositionUpdate{testPosProto}, nil
	}

	return result, nil
}

func (h *PositionHandler) broadcastPositionUpdate(ctx context.Context, update *proto.PositionUpdate) error {
	h.logger.Debug("Converting position update to JSON for broadcast",
		zap.String("userId", update.UserId),
		zap.Float64("lat", update.Latitude),
		zap.Float64("lon", update.Longitude))

	messageData, err := ws.ConvertPositionUpdateToJSON(update)
	if err != nil {
		return fmt.Errorf("failed to convert position update to JSON: %w", err)
	}

	h.logger.Debug("Position update converted to JSON successfully",
		zap.String("userId", update.UserId),
		zap.ByteString("messageData", messageData))

	err = h.wsHub.BroadcastMessage(ctx, messageData)
	if err != nil {
		return fmt.Errorf("failed to broadcast message to clients: %w", err)
	}

	h.logger.Debug("Message broadcast to all clients successfully",
		zap.String("userId", update.UserId))

	return nil
}
