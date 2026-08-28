package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

// Command is a control message from the cloud.
type Command struct {
	Cmd        string          `json:"cmd"`
	StoreID    string          `json:"store_id"`
	IntervalMS int             `json:"interval_ms"`
	Viewers    int             `json:"viewers"`
	PacketHex  string          `json:"packet_hex"`
	TargetID   int             `json:"target_id"`
	Brand      string          `json:"brand"`
	Model      string          `json:"model"`
	ModelID    string          `json:"model_id"`
	Protocol   string          `json:"protocol"`
	State      json.RawMessage `json:"state"`
	// IR learning (LEARN_IR / LEARN_CANCEL)
	SessionID string `json:"session_id"`
	Slot      string `json:"slot"`
	TimeoutS  int    `json:"timeout_s"`
	// DEPLOY_IRDATA carries the learned code bundle inline; the gateway writes
	// it into the firmware dir verbatim and serves it over the OTA HTTP server.
	Bundle json.RawMessage `json:"bundle"`
}

type outMsg struct {
	binary bool
	data   []byte
}

const (
	outboundBuffer = 256
	writeWait      = 10 * time.Second
	pongWait       = 70 * time.Second
	pingPeriod     = 25 * time.Second
	maxBackoff     = 30 * time.Second
)

// CloudLink maintains the outbound WebSocket to the cloud, reconnecting with
// exponential backoff. A single writer goroutine owns the connection, which is
// what gorilla/websocket requires; everything else queues onto out.
type CloudLink struct {
	url       string
	onCommand func(Command)
	onUp      func() // called after each successful connect (flushes the outbox)

	out       chan outMsg
	connected atomic.Bool
	dropped   atomic.Int64

	mu   sync.Mutex
	conn *websocket.Conn
}

func NewCloudLink(url string, onCommand func(Command), onUp func()) *CloudLink {
	return &CloudLink{
		url:       url,
		onCommand: onCommand,
		onUp:      onUp,
		out:       make(chan outMsg, outboundBuffer),
	}
}

func (c *CloudLink) Connected() bool { return c.connected.Load() }
func (c *CloudLink) Dropped() int64  { return c.dropped.Load() }

// SendJSON queues a JSON message. It never blocks: if the link is down or the
// queue is full the message is dropped, which is correct here because minute
// statistics are replayed from the SQLite outbox and live frames are stale
// within a second anyway.
func (c *CloudLink) SendJSON(v any) bool {
	if !c.Connected() {
		return false
	}
	data, err := json.Marshal(v)
	if err != nil {
		slog.Error("could not encode cloud message", "err", err)
		return false
	}
	return c.enqueue(outMsg{data: data})
}

// SendBinary queues a raw sensor frame for the live-stream bypass.
func (c *CloudLink) SendBinary(b []byte) bool {
	if !c.Connected() {
		return false
	}
	return c.enqueue(outMsg{binary: true, data: b})
}

func (c *CloudLink) enqueue(m outMsg) bool {
	select {
	case c.out <- m:
		return true
	default:
		n := c.dropped.Add(1)
		if n%100 == 1 {
			slog.Warn("cloud outbound queue full; dropping messages", "dropped_total", n)
		}
		return false
	}
}

// Run dials and re-dials until the context is cancelled.
func (c *CloudLink) Run(ctx context.Context) {
	backoff := time.Second
	for ctx.Err() == nil {
		if err := c.session(ctx); err != nil && ctx.Err() == nil {
			slog.Warn("cloud link down; retrying", "err", err, "in", backoff)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
		if c.Connected() { // a session that lasted a while resets the backoff
			backoff = time.Second
		}
	}
}

// session runs one connection to completion.
func (c *CloudLink) session(ctx context.Context) error {
	dialCtx, cancelDial := context.WithTimeout(ctx, 10*time.Second)
	defer cancelDial()

	conn, _, err := websocket.DefaultDialer.DialContext(dialCtx, c.url, nil)
	if err != nil {
		return err
	}

	c.mu.Lock()
	c.conn = conn
	c.mu.Unlock()
	c.connected.Store(true)
	slog.Info("cloud link established", "url", redactToken(c.url))

	sessionCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); c.writeLoop(sessionCtx, cancel, conn) }()
	go func() { defer wg.Done(); c.readLoop(cancel, conn) }()

	if c.onUp != nil {
		c.onUp()
	}

	<-sessionCtx.Done()
	_ = conn.Close()
	wg.Wait()

	c.connected.Store(false)
	c.mu.Lock()
	c.conn = nil
	c.mu.Unlock()
	c.drainQueue()
	return nil
}

// drainQueue discards anything still queued for a connection that has gone
// away, so a reconnect does not start by flushing stale live frames.
func (c *CloudLink) drainQueue() {
	for {
		select {
		case <-c.out:
		default:
			return
		}
	}
}

func (c *CloudLink) writeLoop(ctx context.Context, cancel context.CancelFunc, conn *websocket.Conn) {
	defer cancel()
	ticker := time.NewTicker(pingPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case m := <-c.out:
			kind := websocket.TextMessage
			if m.binary {
				kind = websocket.BinaryMessage
			}
			_ = conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := conn.WriteMessage(kind, m.data); err != nil {
				slog.Warn("cloud write failed", "err", err)
				return
			}
		case <-ticker.C:
			if err := conn.WriteControl(websocket.PingMessage, nil,
				time.Now().Add(writeWait)); err != nil {
				return
			}
		}
	}
}

func (c *CloudLink) readLoop(cancel context.CancelFunc, conn *websocket.Conn) {
	defer cancel()
	_ = conn.SetReadDeadline(time.Now().Add(pongWait))
	conn.SetPongHandler(func(string) error {
		return conn.SetReadDeadline(time.Now().Add(pongWait))
	})
	conn.SetPingHandler(func(appData string) error {
		_ = conn.SetReadDeadline(time.Now().Add(pongWait))
		// This runs on the read goroutine while writeLoop owns the socket.
		// WriteControl is the only write method gorilla allows to be called
		// concurrently; WriteMessage here races the writer and panics with
		// "concurrent write to websocket connection".
		err := conn.WriteControl(websocket.PongMessage, []byte(appData),
			time.Now().Add(writeWait))
		if err == websocket.ErrCloseSent {
			return nil
		}
		return err
	})

	for {
		kind, data, err := conn.ReadMessage()
		if err != nil {
			if !websocket.IsCloseError(err, websocket.CloseNormalClosure,
				websocket.CloseGoingAway) {
				slog.Debug("cloud read ended", "err", err)
			}
			return
		}
		_ = conn.SetReadDeadline(time.Now().Add(pongWait))
		if kind != websocket.TextMessage {
			continue
		}
		var cmd Command
		if err := json.Unmarshal(data, &cmd); err != nil {
			slog.Warn("cloud sent malformed JSON", "err", err)
			continue
		}
		if c.onCommand != nil {
			c.onCommand(cmd)
		}
	}
}
