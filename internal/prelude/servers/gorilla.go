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
		c.String(http.StatusUnauthorized, "Missing token")
		return
	}

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

	conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		h.logger.Error("Failed to upgrade to WebSocket", zap.Error(err))
		return
	}
	conn.EnableWriteCompression(true)

	userID := resp.Id
	err = h.wsHub.AddConnection(c.Request.Context(), userID, conn)
	if err != nil {
		h.logger.Error("Failed to add connection", zap.Error(err), zap.String("userID", userID))
		conn.Close()
		return
	}
	defer h.wsHub.RemoveConnection(c.Request.Context(), userID, conn)

	h.logger.Info("User connected via WebSocket", zap.String("userID", userID))

	h.sendInitialPositions(c.Request.Context(), conn)

	go h.readPump(conn, userID)
}

func (h *GorillaHandler) sendInitialPositions(ctx context.Context, conn *websocket.Conn) {
	positionsResp, err := h.positionSvc.GetAllPositions(ctx, &positionProto.GetAllPositionsRequest{})
	if err != nil {
		h.logger.Error("Failed to get initial positions", zap.Error(err))
		return
	}

	positionsData, err := ws.ConvertPositionsToJSON(positionsResp.Positions)
	if err != nil {
		h.logger.Error("Failed to convert positions to JSON", zap.Error(err))
		return
	}

	err = conn.WriteMessage(websocket.TextMessage, positionsData)
	if err != nil {
		h.logger.Error("Failed to send initial positions", zap.Error(err))
	}
}

func (h *GorillaHandler) readPump(conn *websocket.Conn, userID string) {
	defer conn.Close()
	for {
		_, message, err := conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				h.logger.Error("Unexpected WebSocket closure", zap.Error(err))
			}
			break
		}

		h.processClientMessage(message, userID)
	}
}

func (h *GorillaHandler) processClientMessage(message []byte, userID string) {
	var posUpdate struct {
		Type      string  `json:"type"`
		Latitude  float64 `json:"latitude"`
		Longitude float64 `json:"longitude"`
		Speed     float64 `json:"speed"`
		Status    string  `json:"status"`
	}

	if err := json.Unmarshal(message, &posUpdate); err != nil {
		h.logger.Error("Failed to parse client message", zap.Error(err))
		return
	}

	if posUpdate.Type == "position_update" {
		_, err := h.positionSvc.UpdatePosition(context.Background(), &positionProto.PositionUpdate{
			UserId:    userID,
			Latitude:  posUpdate.Latitude,
			Longitude: posUpdate.Longitude,
			Speed:     posUpdate.Speed,
			Status:    posUpdate.Status,
		})
		if err != nil {
			h.logger.Error("Failed to forward position update", zap.Error(err))
		}
	}
}
