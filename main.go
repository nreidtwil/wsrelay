package main

import (
	"bytes"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"

	"github.com/gorilla/websocket"
	"wsrelay/scenarios"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	addr := ":" + port

	http.HandleFunc("/relay/", handleRelay)
	http.HandleFunc("/twiml/", handleTwiML)
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	slog.Info("wsrelay listening", "addr", addr)
	if err := http.ListenAndServe(addr, nil); err != nil {
		fmt.Fprintf(os.Stderr, "wsrelay: %v\n", err)
		os.Exit(1)
	}
}

// handleTwiML serves TwiML XML for a scenario. Twilio fetches this when a call
// arrives on a number configured with a TwiML Application whose VoiceUrl points here.
// The TwiML instructs Twilio to open a Conversation Relay WebSocket back to /relay/<scenario>.
func handleTwiML(w http.ResponseWriter, r *http.Request) {
	scenario := strings.TrimPrefix(r.URL.Path, "/twiml/")
	if scenario == "" || strings.Contains(scenario, "/") {
		http.NotFound(w, r)
		return
	}
	// Verify the scenario exists
	if _, err := scenarios.FS.ReadFile(scenario + ".fountain"); err != nil {
		http.NotFound(w, r)
		return
	}

	// Derive the wss:// relay URL from the incoming request's Host header
	host := r.Host
	twiml := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<Response>
  <Connect>
    <ConversationRelay url="wss://%s/relay/%s" />
  </Connect>
</Response>`, host, scenario)

	w.Header().Set("Content-Type", "application/xml")
	fmt.Fprint(w, twiml)
}

func handleRelay(w http.ResponseWriter, r *http.Request) {
	scenario := strings.TrimPrefix(r.URL.Path, "/relay/")
	if scenario == "" || strings.Contains(scenario, "/") {
		http.NotFound(w, r)
		return
	}

	data, err := scenarios.FS.ReadFile(scenario + ".fountain")
	if err != nil {
		http.NotFound(w, r)
		return
	}

	turns := parseFountain(bytes.NewReader(data))

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		slog.Error("upgrade error", "err", err)
		return
	}

	slog.Info("session started", "scenario", scenario)
	runSession(conn, turns)
}
