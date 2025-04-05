package servers

import (
	"errors"
	"fmt"
	"github.com/gin-gonic/gin"
	authProto "github.com/gisit-triggis/gisit-proto/gen/go/auth/v1"
	positionProto "github.com/gisit-triggis/gisit-proto/gen/go/position/v1"
	"github.com/gisit-triggis/gisit-realtime-backend/internal/app/ws"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"net/http"
	"os"
)

func InitGinServer(logger *zap.Logger, wsHub *ws.WsHub) (*http.Server, func()) {
	authConn, err := initGRPCClient(os.Getenv("AUTH_SERVICE_ADDR"))
	if err != nil {
		logger.Fatal("Failed to connect to auth service", zap.Error(err))
	}

	positionServiceAddr := os.Getenv("POSITION_SERVICE_ADDR")
	if positionServiceAddr == "" {
		positionServiceAddr = fmt.Sprintf("localhost:%s", os.Getenv("GRPC_PORT"))
	}

	positionConn, err := initGRPCClient(positionServiceAddr)
	if err != nil {
		logger.Fatal("Failed to connect to position service", zap.Error(err))
	}

	cleanup := func() {
		if err := authConn.Close(); err != nil {
			logger.Error("Failed to close auth service connection", zap.Error(err))
		}
		if err := positionConn.Close(); err != nil {
			logger.Error("Failed to close position service connection", zap.Error(err))
		}
	}

	authClient := authProto.NewAuthClient(authConn)
	positionClient := positionProto.NewPositionServiceClient(positionConn)

	router := gin.New()
	router.Use(gin.Recovery())

	router.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	router.GET("/metrics", gin.WrapH(promhttp.Handler()))

	gH := NewGorillaHandler(authClient, positionClient, logger, wsHub)
	router.GET("/ws", gH.handleWebSocket)

	httpServer := &http.Server{
		Addr:    fmt.Sprintf(":%s", os.Getenv("HTTP_PORT")),
		Handler: router,
	}

	go func() {
		logger.Info("Starting HTTP server", zap.String("addr", httpServer.Addr))
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Fatal("Failed to start HTTP server", zap.Error(err))
		}
	}()

	return httpServer, cleanup
}

func initGRPCClient(addr string) (*grpc.ClientConn, error) {
	return grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
}
