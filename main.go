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
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	slog.Info("wsrelay listening", "addr", addr)
	if err := http.ListenAndServe(addr, nil); err != nil {
		fmt.Fprintf(os.Stderr, "wsrelay: %v\n", err)
		os.Exit(1)
	}
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
