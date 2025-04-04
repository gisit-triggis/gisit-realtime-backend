package servers

import (
	"github.com/gin-gonic/gin"
	"github.com/gisit-triggis/gisit-realtime-backend/internal/app/ws"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"go.uber.org/zap"
	"net/http"
)

type GorillaHandler struct {
	logger *zap.Logger
	wsHub  *ws.WsHub
}

func NewGorillaHandler(logger *zap.Logger, wsHub *ws.WsHub) *GorillaHandler {
	return &GorillaHandler{
		logger: logger,
		wsHub:  wsHub,
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

	//resp, err := h.authSvc.ValidateToken(c, &authProto.ValidateTokenRequest{
	//	Token: token,
	//})
	//if err != nil {
	//	h.logger.Error("Failed to validate token", zap.Error(err))
	//	st, ok := status.FromError(err)
	//	if ok && st.Code() == codes.Unauthenticated {
	//		c.String(http.StatusUnauthorized, "Invalid token")
	//	} else {
	//		c.String(http.StatusInternalServerError, "Failed to validate token")
	//	}
	//	return
	//}

	conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		h.logger.Error("Failed to upgrade to WebSocket", zap.Error(err))
		return
	}
	conn.EnableWriteCompression(true)

	//userID := resp.UserId
	userID := uuid.New().String()
	h.wsHub.AddConnection(c.Request.Context(), userID, conn)
	defer h.wsHub.RemoveConnection(c.Request.Context(), userID, conn)

	h.logger.Info("User connected via WebSocket", zap.String("userID", userID))
	go h.readPump(conn, userID)
}

func (h *GorillaHandler) readPump(conn *websocket.Conn, userID string) {
	defer conn.Close()
	for {
		_, message, err := conn.ReadMessage()
		if err != nil {
			h.logger.Error("Read error", zap.Error(err))
			break
		}
		h.logger.Info("Received message", zap.String("userID", userID), zap.ByteString("message", message))
	}
}
