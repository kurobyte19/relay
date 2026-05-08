package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	pendingTimeout = 30 * time.Second
	copyBufSize    = 16 * 1024 // 16KB — enough headroom without wasting memory
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin:     func(r *http.Request) bool { return true },
}

var (
	pending = make(map[string]net.Conn)
	mu      sync.Mutex

	// Pool of copy buffers — one reused per active pipe goroutine.
	bufPool = sync.Pool{
		New: func() interface{} {
			b := make([]byte, copyBufSize)
			return &b
		},
	}
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

	// After the WebSocket handshake we drop down to the raw TCP connection.
	// WebSocket frames from each client pass through as opaque bytes —
	// the relay never needs to parse them.
	conn := ws.UnderlyingConn()

	// Disable Nagle — audio frames are small and latency matters more than throughput.
	if tc, ok := conn.(*net.TCPConn); ok {
		tc.SetNoDelay(true)
	}

	var peer net.Conn

	mu.Lock()
	if pendingConn, exists := pending[sessionID]; exists {
		peer = pendingConn
		delete(pending, sessionID)
		log.Printf("[Relay] Session %s fully connected", sessionID)
	} else {
		pending[sessionID] = conn
		log.Printf("[Relay] Session %s waiting for peer", sessionID)
	}
	mu.Unlock()

	if peer != nil {
		go pipe(conn, peer, sessionID)
		go pipe(peer, conn, sessionID)
	} else {
		// First peer waits; clean up if no one joins in time.
		go func() {
			time.Sleep(pendingTimeout)
			mu.Lock()
			if pending[sessionID] == conn {
				log.Printf("[Relay] Session %s timed out", sessionID)
				delete(pending, sessionID)
				conn.Close()
			}
			mu.Unlock()
		}()
	}
}

// pipe copies raw bytes in one direction until either side closes.
// Using raw net.Conn means zero WebSocket framing overhead in the relay.
func pipe(src, dst net.Conn, sessionID string) {
	defer src.Close()
	defer dst.Close()

	buf := bufPool.Get().(*[]byte)
	defer bufPool.Put(buf)

	if _, err := io.CopyBuffer(dst, src, *buf); err != nil {
		log.Printf("[Relay] Session %s pipe closed: %v", sessionID, err)
	}
}
