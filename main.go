package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true // Allow all for prototyping
	},
}

// Session represents a call bridged between two users via relay.
type Session struct {
	UserA *websocket.Conn
	UserB *websocket.Conn
	mu    sync.Mutex
}

var (
	// pending stores users waiting for their counterpart to connect.
	// Key is the unique session ID randomly generated during the call phase.
	pending = make(map[string]*websocket.Conn)
	mu      sync.Mutex
)

func main() {
	port := flag.Int("port", 8081, "Port to run the relay server on")
	flag.Parse()

	http.HandleFunc("/ws", handleConnection)

	log.Printf("Starting Relay Server on :%d\n", *port)
	err := http.ListenAndServe(fmt.Sprintf(":%d", *port), nil)
	if err != nil {
		log.Fatal("ListenAndServe: ", err)
	}
}

func handleConnection(w http.ResponseWriter, r *http.Request) {
	sessionID := r.URL.Query().Get("id")
	if sessionID == "" {
		http.Error(w, "Session ID required", http.StatusBadRequest)
		return
	}

	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println(err)
		return
	}

	var peer *websocket.Conn
	var isFirst bool

	mu.Lock()
	if pendingConn, exists := pending[sessionID]; exists {
		// Second user arrived! Match them up.
		peer = pendingConn
		delete(pending, sessionID)
		isFirst = false
		log.Printf("[Relay] Session %s fully connected.", sessionID)
	} else {
		// First user arrived. Wait for the other.
		pending[sessionID] = ws
		isFirst = true
		log.Printf("[Relay] Session %s initialized by first caller. Waiting for peer...", sessionID)
	}
	mu.Unlock()

	// If this is the second person joining, start bi-directional forwarding.
	if !isFirst && peer != nil {
		go bridge(ws, peer, sessionID)
		go bridge(peer, ws, sessionID)
	} else {
		// First person joins and waits. If the second person doesn't arrive soon, clean up.
		go func() {
			time.Sleep(30 * time.Second)
			mu.Lock()
			if pending[sessionID] == ws {
				log.Printf("[Relay] Session %s timed out waiting for peer.", sessionID)
				delete(pending, sessionID)
				ws.Close()
			}
			mu.Unlock()
		}()
	}
}

// bridge blindly forwards every message type between User A and User B until one drops.
func bridge(src, dst *websocket.Conn, sessionID string) {
	defer src.Close()
	defer dst.Close()

	for {
		msgType, r, err := src.NextReader()
		if err != nil {
			log.Printf("[Relay] Session %s broken. Connection closed.", sessionID)
			return
		}

		w, err := dst.NextWriter(msgType)
		if err != nil {
			return
		}

		if _, err := io.Copy(w, r); err != nil {
			return
		}

		if err := w.Close(); err != nil {
			return
		}
	}
}
