package ws

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
	"log"
	"sync"
)

const (
	RedisChannel   = "ws-broadcast"
	UserNodeMapKey = "user_node_map"
)

type MsgPayload struct {
	UserID  string `json:"user_id"`
	Message []byte `json:"message"`
	From    string `json:"from_node"`
	To      string `json:"to_node"`
}

type WsHub struct {
	nodeID string

	rdb *redis.Client

	mu          sync.RWMutex
	connections map[string][]*websocket.Conn
}

func NewWsHub(nodeID string, redisClient *redis.Client) *WsHub {
	hub := &WsHub{
		nodeID:      nodeID,
		rdb:         redisClient,
		connections: make(map[string][]*websocket.Conn),
	}

	go hub.subscribePubSub()

	return hub
}

func (h *WsHub) AddConnection(ctx context.Context, userID string, conn *websocket.Conn) error {
	err := h.rdb.HSet(ctx, UserNodeMapKey, userID, h.nodeID).Err()
	if err != nil {
		return fmt.Errorf("failed to set user->node in redis: %w", err)
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	h.connections[userID] = append(h.connections[userID], conn)
	return nil
}

func (h *WsHub) RemoveConnection(ctx context.Context, userID string, conn *websocket.Conn) {
	h.mu.Lock()
	defer h.mu.Unlock()

	conns := h.connections[userID]
	for i, c := range conns {
		if c == conn {
			conns = append(conns[:i], conns[i+1:]...)
			break
		}
	}
	if len(conns) == 0 {
		delete(h.connections, userID)
	} else {
		h.connections[userID] = conns
	}
}

func (h *WsHub) SendMessageToUser(ctx context.Context, userID string, message []byte) error {
	nodeID, err := h.rdb.HGet(ctx, UserNodeMapKey, userID).Result()
	if err == redis.Nil {
		return errors.New("user not found in redis")
	} else if err != nil {
		return fmt.Errorf("redis error: %w", err)
	}

	if nodeID == h.nodeID {
		return h.sendLocal(userID, message)
	}

	payload := MsgPayload{
		UserID:  userID,
		Message: message,
		From:    h.nodeID,
		To:      nodeID,
	}
	data, _ := json.Marshal(payload)
	return h.rdb.Publish(ctx, RedisChannel, data).Err()
}

func (h *WsHub) sendLocal(userID string, message []byte) error {
	h.mu.RLock()
	defer h.mu.RUnlock()

	conns, ok := h.connections[userID]
	if !ok || len(conns) == 0 {
		return errors.New("user not connected on this node")
	}

	for _, c := range conns {
		err := c.WriteMessage(websocket.TextMessage, message)
		if err != nil {
			log.Printf("Send error to userID=%s: %v\n", userID, err)
		}
	}
	return nil
}

func (h *WsHub) subscribePubSub() {
	ctx := context.Background()
	sub := h.rdb.Subscribe(ctx, RedisChannel)
	ch := sub.Channel()

	for msg := range ch {
		var payload MsgPayload
		if err := json.Unmarshal([]byte(msg.Payload), &payload); err != nil {
			log.Println("failed to unmarshal pubsub payload:", err)
			continue
		}

		if payload.To == h.nodeID {
			_ = h.sendLocal(payload.UserID, payload.Message)
		}
	}
}
