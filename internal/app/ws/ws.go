package ws

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	proto "github.com/gisit-triggis/gisit-proto/gen/go/position/v1"
	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
	"log"
	"sync"
)

const (
	RedisChannel     = "ws-broadcast"
	UserNodeMapKey   = "user_node_map"
	BroadcastChannel = "broadcast-all"
)

type MsgPayload struct {
	UserID  string `json:"user_id"`
	Message []byte `json:"message"`
	From    string `json:"from_node"`
	To      string `json:"to_node"`
}

type PositionMessage struct {
	Type      string  `json:"type"`
	UserID    string  `json:"user_id"`
	Lat       float64 `json:"lat"`
	Lon       float64 `json:"lon"`
	Speed     float64 `json:"speed"`
	Status    string  `json:"status"`
	Timestamp string  `json:"timestamp"`
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
		_, err := h.rdb.HDel(ctx, UserNodeMapKey, userID).Result()
		if err != nil {
			log.Printf("WARN: Failed to remove user %s from node map in Redis: %v", userID, err)
		}
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

func (h *WsHub) BroadcastMessage(ctx context.Context, message []byte) error {
	h.broadcastLocal(message)

	payload := MsgPayload{
		UserID:  "",
		Message: message,
		From:    h.nodeID,
		To:      "",
	}
	data, _ := json.Marshal(payload)
	return h.rdb.Publish(ctx, BroadcastChannel, data).Err()
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

func (h *WsHub) broadcastLocal(message []byte) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	for userID, conns := range h.connections {
		for _, c := range conns {
			err := c.WriteMessage(websocket.TextMessage, message)
			if err != nil {
				log.Printf("Broadcast error to userID=%s: %v\n", userID, err)
			}
		}
	}
}

func (h *WsHub) subscribePubSub() {
	ctx := context.Background()

	directSub := h.rdb.Subscribe(ctx, RedisChannel)
	directCh := directSub.Channel()

	broadcastSub := h.rdb.Subscribe(ctx, BroadcastChannel)
	broadcastCh := broadcastSub.Channel()

	go func() {
		for msg := range directCh {
			var payload MsgPayload
			if err := json.Unmarshal([]byte(msg.Payload), &payload); err != nil {
				log.Println("failed to unmarshal pubsub direct payload:", err)
				continue
			}

			if payload.To == h.nodeID {
				_ = h.sendLocal(payload.UserID, payload.Message)
			}
		}
	}()

	go func() {
		for msg := range broadcastCh {
			var payload MsgPayload
			if err := json.Unmarshal([]byte(msg.Payload), &payload); err != nil {
				log.Println("failed to unmarshal pubsub broadcast payload:", err)
				continue
			}

			if payload.From != h.nodeID {
				h.broadcastLocal(payload.Message)
			}
		}
	}()
}

func ConvertPositionUpdateToJSON(update *proto.PositionUpdate) ([]byte, error) {
	message := PositionMessage{
		Type:      "position_update",
		UserID:    update.UserId,
		Lat:       update.Latitude,
		Lon:       update.Longitude,
		Speed:     update.Speed,
		Status:    update.Status,
		Timestamp: update.Timestamp,
	}

	return json.Marshal(message)
}

func ConvertPositionsToJSON(updates []*proto.PositionUpdate) ([]byte, error) {
	positions := make([]PositionMessage, len(updates))

	for i, update := range updates {
		positions[i] = PositionMessage{
			Type:      "position_update",
			UserID:    update.UserId,
			Lat:       update.Latitude,
			Lon:       update.Longitude,
			Speed:     update.Speed,
			Status:    update.Status,
			Timestamp: update.Timestamp,
		}
	}

	message := struct {
		Type      string            `json:"type"`
		Positions []PositionMessage `json:"positions"`
	}{
		Type:      "initial_positions",
		Positions: positions,
	}

	return json.Marshal(message)
}
