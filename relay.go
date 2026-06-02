package main

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// inboundMsg is a message received from Twilio Conversation Relay.
type inboundMsg struct {
	Type   string `json:"type"`
	Speech string `json:"speech,omitempty"`
	Last   bool   `json:"last,omitempty"`
	Reason string `json:"reason,omitempty"`
}

// outboundMsg is a message sent to Twilio Conversation Relay.
type outboundMsg struct {
	Type  string `json:"type"`
	Token string `json:"token,omitempty"`
	Last  bool   `json:"last,omitempty"`
}

const wordsPerToken = 5

func tokenise(dialogue string) []string {
	words := strings.Fields(dialogue)
	var tokens []string
	for i := 0; i < len(words); i += wordsPerToken {
		end := i + wordsPerToken
		if end > len(words) {
			end = len(words)
		}
		tokens = append(tokens, strings.Join(words[i:end], " "))
	}
	return tokens
}

// runSession drives a single Conversation Relay WebSocket session.
// It streams each agent turn as text tokens and handles barge-in.
func runSession(conn *websocket.Conn, turns []Turn) {
	defer conn.Close()

	inbound := make(chan *inboundMsg, 16)
	readerDone := make(chan error, 1)
	var closeOnce sync.Once

	go func() {
		for {
			_, raw, err := conn.ReadMessage()
			if err != nil {
				closeOnce.Do(func() { readerDone <- err; close(inbound) })
				return
			}
			var msg inboundMsg
			if json.Unmarshal(raw, &msg) == nil {
				select {
				case inbound <- &msg:
				default:
				}
			}
		}
	}()

	for _, turn := range turns {
		// Only stream agent turns — skip human/caller characters.
		if turn.Character == "AVA AGENT" || turn.Character == "HUGH HUMAN" {
			if err := streamTurn(conn, inbound, turn); err != nil {
				slog.Error("session error", "turn", turn.Character, "err", err)
				return
			}
		}
	}

	end, _ := json.Marshal(outboundMsg{Type: "end"})
	_ = conn.WriteMessage(websocket.TextMessage, end)

	slog.Info("session complete")
}

func streamTurn(conn *websocket.Conn, inbound <-chan *inboundMsg, turn Turn) error {
	// Brief hesitation before speaking.
	time.Sleep(150 * time.Millisecond)

	tokens := tokenise(turn.Dialogue)
	interrupted := false

	// Drain stale messages.
drain:
	for {
		select {
		case <-inbound:
		default:
			break drain
		}
	}

	for idx, tok := range tokens {
		select {
		case msg, ok := <-inbound:
			if !ok {
				return fmt.Errorf("connection closed")
			}
			if msg != nil && msg.Type == "interrupted" {
				interrupted = true
			}
		default:
		}
		if interrupted {
			break
		}
		b, _ := json.Marshal(outboundMsg{
			Type:  "text",
			Token: tok,
			Last:  idx == len(tokens)-1,
		})
		if err := conn.WriteMessage(websocket.TextMessage, b); err != nil {
			return err
		}
	}

	if interrupted {
		return nil
	}

	// Wait for speech acknowledgement.
	for {
		msg, ok := <-inbound
		if !ok {
			return fmt.Errorf("connection closed waiting for ack")
		}
		if msg.Type == "speech" && msg.Last {
			return nil
		}
		if msg.Type == "interrupted" {
			return nil
		}
	}
}
