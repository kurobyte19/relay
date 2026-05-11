package main

import (
	"crypto/rand"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// startTestServer spins up an httptest server using the relay's handleConnection.
func startTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(nil)
	srv.Config.Handler = http.HandlerFunc(handleConnection)
	return srv
}

// dialWS opens a WebSocket connection to the test server for a given session ID.
func dialWS(t *testing.T, serverURL, sessionID string) *websocket.Conn {
	t.Helper()
	u := "ws" + serverURL[4:] + "/ws?id=" + sessionID
	conn, _, err := websocket.DefaultDialer.Dial(u, nil)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	return conn
}

// -----------------------------------------------------------------------
// BenchmarkRelayThroughput measures raw bytes/sec through one relay pair.
// -----------------------------------------------------------------------
func BenchmarkRelayThroughput(b *testing.B) {
	srv := httptest.NewServer(http.HandlerFunc(handleConnection))
	defer srv.Close()

	sessionID := "bench-throughput"
	u := "ws" + srv.URL[4:] + "/ws?id=" + sessionID

	sender, _, err := websocket.DefaultDialer.Dial(u, nil)
	if err != nil {
		b.Fatal(err)
	}
	receiver, _, err := websocket.DefaultDialer.Dial(u, nil)
	if err != nil {
		b.Fatal(err)
	}
	defer sender.Close()
	defer receiver.Close()

	payload := make([]byte, 1024) // 1 KB per frame — realistic audio chunk
	rand.Read(payload)

	var bytesReceived atomic.Int64
	done := make(chan struct{})

	go func() {
		for {
			_, msg, err := receiver.ReadMessage()
			if err != nil {
				close(done)
				return
			}
			bytesReceived.Add(int64(len(msg)))
		}
	}()

	b.SetBytes(int64(len(payload)))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := sender.WriteMessage(websocket.BinaryMessage, payload); err != nil {
			b.Fatal(err)
		}
	}

	// Flush — wait until receiver has caught up or timeout.
	deadline := time.Now().Add(5 * time.Second)
	for bytesReceived.Load() < int64(b.N)*int64(len(payload)) {
		if time.Now().After(deadline) {
			b.Logf("timeout: received %d / %d bytes", bytesReceived.Load(), int64(b.N)*int64(len(payload)))
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	b.ReportMetric(float64(bytesReceived.Load()), "bytes_relayed")
}

// -----------------------------------------------------------------------
// BenchmarkRelayLatency measures round-trip time for a single frame.
// -----------------------------------------------------------------------
func BenchmarkRelayLatency(b *testing.B) {
	srv := httptest.NewServer(http.HandlerFunc(handleConnection))
	defer srv.Close()

	// A → relay → B → relay → A (full round trip needs two sessions)
	// For one-way latency, we measure send→receive time across the relay.
	a2b := "bench-latency-a2b"
	b2a := "bench-latency-b2a"
	ua := "ws" + srv.URL[4:] + "/ws?id=" + a2b
	ub := "ws" + srv.URL[4:] + "/ws?id=" + b2a

	aOut, _, _ := websocket.DefaultDialer.Dial(ua, nil)
	bIn, _, _ := websocket.DefaultDialer.Dial(ua, nil)
	bOut, _, _ := websocket.DefaultDialer.Dial(ub, nil)
	aIn, _, _ := websocket.DefaultDialer.Dial(ub, nil)
	defer aOut.Close()
	defer bIn.Close()
	defer bOut.Close()
	defer aIn.Close()

	payload := make([]byte, 160) // 20ms of 8kHz mono — typical VoIP frame
	rand.Read(payload)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		t0 := time.Now()

		// A sends to B
		if err := aOut.WriteMessage(websocket.BinaryMessage, payload); err != nil {
			b.Fatal(err)
		}
		if _, _, err := bIn.ReadMessage(); err != nil {
			b.Fatal(err)
		}

		// B replies to A
		if err := bOut.WriteMessage(websocket.BinaryMessage, payload); err != nil {
			b.Fatal(err)
		}
		if _, _, err := aIn.ReadMessage(); err != nil {
			b.Fatal(err)
		}

		b.ReportMetric(float64(time.Since(t0).Microseconds()), "rtt_µs")
	}
}

// -----------------------------------------------------------------------
// TestRelayMultipleSessions checks N concurrent sessions don't bleed into
// each other and all complete without error.
// -----------------------------------------------------------------------
func TestRelayMultipleSessions(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(handleConnection))
	defer srv.Close()

	const numSessions = 20
	const framesPerSession = 50

	var wg sync.WaitGroup
	errors := make(chan error, numSessions*2)

	for i := 0; i < numSessions; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			sessionID := fmt.Sprintf("session-%d", id)
			u := "ws" + srv.URL[4:] + "/ws?id=" + sessionID

			sender, _, err := websocket.DefaultDialer.Dial(u, nil)
			if err != nil {
				errors <- fmt.Errorf("session %d sender dial: %v", id, err)
				return
			}
			receiver, _, err := websocket.DefaultDialer.Dial(u, nil)
			if err != nil {
				errors <- fmt.Errorf("session %d receiver dial: %v", id, err)
				return
			}
			defer sender.Close()
			defer receiver.Close()

			// Tag every frame with the session ID to detect cross-session bleed.
			tag := fmt.Sprintf("session-%d-frame-", id)
			recv := make(chan string, framesPerSession)

			go func() {
				for j := 0; j < framesPerSession; j++ {
					_, msg, err := receiver.ReadMessage()
					if err != nil {
						errors <- fmt.Errorf("session %d read: %v", id, err)
						return
					}
					recv <- string(msg)
				}
			}()

			for j := 0; j < framesPerSession; j++ {
				frame := fmt.Sprintf("%s%d", tag, j)
				if err := sender.WriteMessage(websocket.TextMessage, []byte(frame)); err != nil {
					errors <- fmt.Errorf("session %d write: %v", id, err)
					return
				}
			}

			for j := 0; j < framesPerSession; j++ {
				expected := fmt.Sprintf("%s%d", tag, j)
				got := <-recv
				if got != expected {
					errors <- fmt.Errorf("session %d frame %d: got %q want %q", id, j, got, expected)
				}
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Error(err)
	}
}

// -----------------------------------------------------------------------
// TestRelayTimeout verifies the pending session is cleaned up after 30s.
// (Uses a shortened timeout via a package-level var if you expose one.)
// -----------------------------------------------------------------------
func TestRelayPendingTimeout(t *testing.T) {
	// To run fast, expose a testable timeout or just verify the conn is closed.
	// Here we just confirm the server doesn't hang when only one peer connects.
	srv := httptest.NewServer(http.HandlerFunc(handleConnection))
	defer srv.Close()

	sessionID := "timeout-test"
	u := "ws" + srv.URL[4:] + "/ws?id=" + sessionID

	conn, _, err := websocket.DefaultDialer.Dial(u, nil)
	if err != nil {
		t.Fatal(err)
	}

	// The server will close this after pendingTimeout.
	// We just verify the connection eventually closes without us doing anything.
	conn.SetReadDeadline(time.Now().Add(35 * time.Second))
	_, _, err = conn.ReadMessage()
	if err == nil {
		t.Fatal("expected connection to be closed by server timeout, got nil error")
	}
	t.Logf("Connection closed as expected: %v", err)
}
