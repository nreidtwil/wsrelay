package main

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

// pendingAgent holds an outbound connection from twilsynth run waiting to be
// paired with an inbound Twilio call.
type pendingAgent struct {
	conn      *websocket.Conn
	scenario  string
	paired    chan struct{} // closed when Twilio pairs with this agent
	connDone  chan struct{} // closed when proxy finishes (conn can be released)
	createdAt time.Time
}

var (
	agents     sync.Map // runID → *pendingAgent
	byScenario sync.Map // scenario → *scenarioQueue
)

type scenarioQueue struct {
	mu     sync.Mutex
	runIDs []string
}

func queueFor(scenario string) *scenarioQueue {
	v, _ := byScenario.LoadOrStore(scenario, &scenarioQueue{})
	return v.(*scenarioQueue)
}

func (q *scenarioQueue) push(runID string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.runIDs = append(q.runIDs, runID)
}

// pop returns the oldest run-ID that still has a live agent connection, skipping stale entries.
func (q *scenarioQueue) pop() string {
	q.mu.Lock()
	defer q.mu.Unlock()
	for len(q.runIDs) > 0 {
		id := q.runIDs[0]
		q.runIDs = q.runIDs[1:]
		if _, ok := agents.Load(id); ok {
			return id
		}
		// stale — agent disconnected before Twilio called; skip
	}
	return ""
}

func (q *scenarioQueue) remove(runID string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	filtered := q.runIDs[:0]
	for _, id := range q.runIDs {
		if id != runID {
			filtered = append(filtered, id)
		}
	}
	q.runIDs = filtered
}

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "9099"
	}
	relayHost := os.Getenv("RELAY_HOST")
	if relayHost == "" {
		relayHost = "relay.sierrita.dev"
	}
	timeoutSecs := 300
	if v := os.Getenv("AGENT_TIMEOUT_SECS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			timeoutSecs = n
		}
	}

	http.HandleFunc("/agent/", func(w http.ResponseWriter, r *http.Request) {
		handleAgent(w, r, time.Duration(timeoutSecs)*time.Second)
	})
	http.HandleFunc("/relay/", handleRelay)
	http.HandleFunc("/twiml/", func(w http.ResponseWriter, r *http.Request) {
		handleTwiML(w, r, relayHost)
	})
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	slog.Info("wsrelay listening", "addr", ":"+port, "relay_host", relayHost)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		fmt.Fprintf(os.Stderr, "wsrelay: %v\n", err)
		os.Exit(1)
	}
}

// handleAgent accepts an outbound WebSocket from twilsynth run and holds it
// until Twilio pairs with it or the timeout fires.
func handleAgent(w http.ResponseWriter, r *http.Request, timeout time.Duration) {
	runID := strings.TrimPrefix(r.URL.Path, "/agent/")
	if runID == "" || strings.Contains(runID, "/") {
		http.Error(w, "invalid run-id", http.StatusBadRequest)
		return
	}
	scenario := r.URL.Query().Get("scenario")
	if scenario == "" {
		http.Error(w, "scenario query param required", http.StatusBadRequest)
		return
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		slog.Error("agent upgrade error", "err", err)
		return
	}

	agent := &pendingAgent{
		conn:      conn,
		scenario:  scenario,
		paired:    make(chan struct{}),
		connDone:  make(chan struct{}),
		createdAt: time.Now(),
	}
	agents.Store(runID, agent)
	queueFor(scenario).push(runID)
	slog.Info("agent registered", "run_id", runID, "scenario", scenario)

	defer func() {
		agents.Delete(runID)
		queueFor(scenario).remove(runID)
		conn.Close()
		slog.Info("agent removed", "run_id", runID)
	}()

	select {
	case <-agent.paired:
		// Proxy is running in handleRelay; wait for it to finish before cleanup.
		<-agent.connDone
	case <-time.After(timeout):
		slog.Warn("agent timed out", "run_id", runID)
		conn.WriteMessage(websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseNormalClosure, "timeout"))
	}
}

// handleRelay accepts an inbound WebSocket from Twilio, pairs it with the
// waiting agent, and proxies frames bidirectionally.
func handleRelay(w http.ResponseWriter, r *http.Request) {
	runID := strings.TrimPrefix(r.URL.Path, "/relay/")
	if runID == "" || strings.Contains(runID, "/") {
		http.NotFound(w, r)
		return
	}

	val, ok := agents.LoadAndDelete(runID)
	if !ok {
		http.Error(w, "no agent waiting for this run-id", http.StatusNotFound)
		return
	}
	agent := val.(*pendingAgent)
	queueFor(agent.scenario).remove(runID)

	twilioConn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		slog.Error("twilio upgrade error", "err", err)
		// Put agent back so it can time out gracefully.
		agents.Store(runID, agent)
		return
	}

	close(agent.paired)
	slog.Info("paired", "run_id", runID, "scenario", agent.scenario)

	done1 := make(chan struct{})
	done2 := make(chan struct{})
	go func() {
		defer close(done1)
		proxyFrames(twilioConn, agent.conn)
	}()
	go func() {
		defer close(done2)
		proxyFrames(agent.conn, twilioConn)
	}()
	<-done1
	<-done2
	twilioConn.Close()
	close(agent.connDone)
	slog.Info("session complete", "run_id", runID)
}

// proxyFrames copies WebSocket frames from src to dst until either closes.
func proxyFrames(src, dst *websocket.Conn) {
	for {
		msgType, msg, err := src.ReadMessage()
		if err != nil {
			dst.Close()
			return
		}
		if err := dst.WriteMessage(msgType, msg); err != nil {
			src.Close()
			return
		}
	}
}

// handleTwiML serves TwiML XML with the oldest waiting run-ID for the scenario.
func handleTwiML(w http.ResponseWriter, r *http.Request, relayHost string) {
	scenario := strings.TrimPrefix(r.URL.Path, "/twiml/")
	if scenario == "" || strings.Contains(scenario, "/") {
		http.NotFound(w, r)
		return
	}

	runID := queueFor(scenario).pop()
	if runID == "" {
		slog.Warn("no agent waiting for twiml request", "scenario", scenario)
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusServiceUnavailable)
		fmt.Fprint(w, `<?xml version="1.0" encoding="UTF-8"?><Response><Reject reason="busy"/></Response>`)
		return
	}

	w.Header().Set("Content-Type", "application/xml")
	fmt.Fprintf(w, "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Response>\n  <Connect>\n    <ConversationRelay url=\"wss://%s/relay/%s\" />\n  </Connect>\n</Response>", relayHost, runID)
	slog.Info("twiml served", "scenario", scenario, "run_id", runID)
}

// newRunID generates a cryptographically random 16-byte hex run identifier.
func newRunID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
