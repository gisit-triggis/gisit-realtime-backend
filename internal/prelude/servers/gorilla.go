package servers

import (
	"context"
	"encoding/json"
	"github.com/gin-gonic/gin"
	authProto "github.com/gisit-triggis/gisit-proto/gen/go/auth/v1"
	positionProto "github.com/gisit-triggis/gisit-proto/gen/go/position/v1"
	"github.com/gisit-triggis/gisit-realtime-backend/internal/app/ws"
	"github.com/gorilla/websocket"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"net/http"
	"time"
)

type GorillaHandler struct {
	authSvc     authProto.AuthClient
	positionSvc positionProto.PositionServiceClient
	logger      *zap.Logger
	wsHub       *ws.WsHub
}

func NewGorillaHandler(
	authSvc authProto.AuthClient,
	positionSvc positionProto.PositionServiceClient,
	logger *zap.Logger,
	wsHub *ws.WsHub,
) *GorillaHandler {
	return &GorillaHandler{
		authSvc:     authSvc,
		positionSvc: positionSvc,
		logger:      logger,
		wsHub:       wsHub,
	}
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
	EnableCompression: true,
}

func (h *GorillaHandler) handleWebSocket(c *gin.Context) {
	token := c.Query("token")
	if token == "" {
		h.logger.Error("Missing token in request")
		c.String(http.StatusUnauthorized, "Missing token")
		return
	}

	h.logger.Info("Authenticating WebSocket connection", zap.String("token", token[:10]+"..."))

	resp, err := h.authSvc.AuthorizeByToken(c, &authProto.AuthorizeByTokenRequest{
		Token: token,
	})
	if err != nil {
		h.logger.Error("Failed to validate token", zap.Error(err))
		st, ok := status.FromError(err)
		if ok && st.Code() == codes.Unauthenticated {
			c.String(http.StatusUnauthorized, "Invalid token")
		} else {
			c.String(http.StatusInternalServerError, "Failed to validate token")
		}
		return
	}

	h.logger.Info("Token validated, upgrading to WebSocket", zap.String("userID", resp.Id))

	conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		h.logger.Error("Failed to upgrade to WebSocket", zap.Error(err), zap.String("userID", resp.Id))
		return
	}
	conn.EnableWriteCompression(true)

	userID := resp.Id
	h.logger.Info("Adding user connection to WebSocket hub", zap.String("userID", userID))

	err = h.wsHub.AddConnection(c.Request.Context(), userID, conn)
	if err != nil {
		h.logger.Error("Failed to add connection to hub", zap.Error(err), zap.String("userID", userID))
		conn.Close()
		return
	}

	defer func() {
		h.logger.Info("Removing WebSocket connection", zap.String("userID", userID))
		h.wsHub.RemoveConnection(c.Request.Context(), userID, conn)
	}()

	h.logger.Info("User connected via WebSocket", zap.String("userID", userID))

	go h.pingConnection(conn, userID)

	h.sendInitialPositions(c.Request.Context(), conn)

	h.readPump(conn, userID)
}

func (h *GorillaHandler) pingConnection(conn *websocket.Conn, userID string) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := conn.WriteControl(websocket.PingMessage, []byte{}, time.Now().Add(10*time.Second)); err != nil {
				h.logger.Info("Ping failed, closing connection", zap.String("userID", userID), zap.Error(err))
				conn.Close()
				return
			}
			h.logger.Debug("Ping sent", zap.String("userID", userID))
		}
	}
}

func (h *GorillaHandler) sendInitialPositions(ctx context.Context, conn *websocket.Conn) {
	h.logger.Info("Fetching initial positions")
	positionsResp, err := h.positionSvc.GetAllPositions(ctx, &positionProto.GetAllPositionsRequest{})
	if err != nil {
		h.logger.Error("Failed to get initial positions", zap.Error(err))
		return
	}

	if len(positionsResp.Positions) == 0 {
		h.logger.Info("No initial positions to send to client")

		emptyPositionsMessage := struct {
			Type      string `json:"type"`
			Positions []any  `json:"positions"`
		}{
			Type:      "initial_positions",
			Positions: []any{},
		}

		data, err := json.Marshal(emptyPositionsMessage)
		if err != nil {
			h.logger.Error("Failed to create empty positions message", zap.Error(err))
			return
		}

		err = conn.WriteMessage(websocket.TextMessage, data)
		if err != nil {
			h.logger.Error("Failed to send empty positions message", zap.Error(err))
		} else {
			h.logger.Info("Empty positions message sent successfully")
		}
		return
	}

	positionsData, err := ws.ConvertPositionsToJSON(positionsResp.Positions)
	if err != nil {
		h.logger.Error("Failed to convert positions to JSON", zap.Error(err))
		return
	}

	h.logger.Info("Sending initial positions to client",
		zap.Int("positionsCount", len(positionsResp.Positions)),
		zap.Int("dataSize", len(positionsData)))

	err = conn.WriteMessage(websocket.TextMessage, positionsData)
	if err != nil {
		h.logger.Error("Failed to send initial positions", zap.Error(err))
	} else {
		h.logger.Info("Initial positions sent successfully")
	}
}

func (h *GorillaHandler) readPump(conn *websocket.Conn, userID string) {
	defer conn.Close()
	h.logger.Info("Starting WebSocket read pump", zap.String("userID", userID))

	conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})

	for {
		conn.SetReadDeadline(time.Now().Add(60 * time.Second))

		_, message, err := conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				h.logger.Error("Unexpected WebSocket closure",
					zap.Error(err),
					zap.String("userID", userID))
			} else {
				h.logger.Info("WebSocket connection closed",
					zap.String("userID", userID),
					zap.Error(err))
			}
			break
		}

		h.logger.Debug("Received WebSocket message",
			zap.String("userID", userID),
			zap.ByteString("message", message))

		h.processClientMessage(message, userID)
	}

	h.logger.Info("WebSocket read pump ended", zap.String("userID", userID))
}

func (h *GorillaHandler) processClientMessage(message []byte, userID string) {
	h.logger.Debug("Processing client message",
		zap.String("userID", userID),
		zap.ByteString("message", message))

	var posUpdate struct {
		Type      string  `json:"type"`
		Latitude  float64 `json:"latitude"`
		Longitude float64 `json:"longitude"`
		Speed     float64 `json:"speed"`
		Status    string  `json:"status"`
	}

	if err := json.Unmarshal(message, &posUpdate); err != nil {
		h.logger.Error("Failed to parse client message",
			zap.Error(err),
			zap.String("userID", userID))
		return
	}

	h.logger.Debug("Parsed client message",
		zap.String("userID", userID),
		zap.String("type", posUpdate.Type))

	if posUpdate.Type == "position_update" {
		now := time.Now().Format(time.RFC3339)
		h.logger.Info("Forwarding position update from WebSocket client",
			zap.String("userID", userID),
			zap.Float64("latitude", posUpdate.Latitude),
			zap.Float64("longitude", posUpdate.Longitude))

		_, err := h.positionSvc.UpdatePosition(context.Background(), &positionProto.PositionUpdate{
			UserId:    userID,
			Latitude:  posUpdate.Latitude,
			Longitude: posUpdate.Longitude,
			Speed:     posUpdate.Speed,
			Status:    posUpdate.Status,
			Timestamp: now,
		})
		if err != nil {
			h.logger.Error("Failed to forward position update",
				zap.Error(err),
				zap.String("userID", userID))
		} else {
			h.logger.Info("Position update from WebSocket forwarded successfully",
				zap.String("userID", userID))
		}
	} else {
		h.logger.Warn("Received unknown message type",
			zap.String("userID", userID),
			zap.String("type", posUpdate.Type))
	}
}
