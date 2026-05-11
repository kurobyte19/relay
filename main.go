package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	pendingTimeout = 30 * time.Second
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  4096,
	WriteBufferSize: 4096,
	CheckOrigin:     func(r *http.Request) bool { return true },
}

var (
	pending = make(map[string]*websocket.Conn)
	mu      sync.Mutex
)

func main() {
	port := flag.Int("port", 8081, "Port to run the relay server on")
	flag.Parse()

	http.HandleFunc("/ws", handleConnection)
	log.Printf("Relay server listening on :%d\n", *port)
	if err := http.ListenAndServe(fmt.Sprintf(":%d", *port), nil); err != nil {
		log.Fatal(err)
	}
}

func handleConnection(w http.ResponseWriter, r *http.Request) {
	sessionID := r.URL.Query().Get("id")
	if sessionID == "" {
		http.Error(w, "session ID required", http.StatusBadRequest)
		return
	}

	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("[Relay] Upgrade error:", err)
		return
	}

	var peer *websocket.Conn

	mu.Lock()
	if pendingConn, exists := pending[sessionID]; exists {
		peer = pendingConn
		delete(pending, sessionID)
		log.Printf("[Relay] Session %s fully connected", sessionID)
	} else {
		pending[sessionID] = ws
		log.Printf("[Relay] Session %s waiting for peer", sessionID)
	}
	mu.Unlock()

	if peer != nil {
		go pipe(ws, peer, sessionID)
		go pipe(peer, ws, sessionID)
	} else {
		// First peer waits; clean up if no one joins in time.
		go func() {
			time.Sleep(pendingTimeout)
			mu.Lock()
			if pending[sessionID] == ws {
				log.Printf("[Relay] Session %s timed out", sessionID)
				delete(pending, sessionID)
				ws.Close()
			}
			mu.Unlock()
		}()
	}
}

// pipe safely reads WebSocket messages from src and writes them to dst.
// This ensures frames are properly unmasked and re-framed according to RFC 6455.
func pipe(src, dst *websocket.Conn, sessionID string) {
	defer src.Close()
	defer dst.Close()

	for {
		msgType, data, err := src.ReadMessage()
		if err != nil {
			log.Printf("[Relay] Session %s pipe closed", sessionID)
			break
		}
		if err := dst.WriteMessage(msgType, data); err != nil {
			log.Printf("[Relay] Session %s write error: %v", sessionID, err)
			break
		}
	}
}