package main

import (
	"bufio"
	"io"
	"strings"
)

// Turn is a single dialogue turn from a Fountain script.
type Turn struct {
	Character string
	Dialogue  string
}

// parseFountain extracts dialogue turns from a Fountain screenplay source.
// Only character + dialogue pairs are returned; transitions and other lines are skipped.
func parseFountain(r io.Reader) []Turn {
	scanner := bufio.NewScanner(r)
	var turns []Turn
	var currentChar string
	var dialogueBuf strings.Builder

	flush := func() {
		if currentChar != "" && dialogueBuf.Len() > 0 {
			turns = append(turns, Turn{
				Character: currentChar,
				Dialogue:  strings.TrimSpace(dialogueBuf.String()),
			})
		}
		currentChar = ""
		dialogueBuf.Reset()
	}

	for scanner.Scan() {
		line := scanner.Text()

		// Blank line ends the current turn.
		if strings.TrimSpace(line) == "" {
			flush()
			continue
		}

		// Skip KDL parenthetical overrides and Fountain transitions.
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "(") || strings.HasPrefix(trimmed, ">") {
			continue
		}

		// A line that is all uppercase (and not a known header) is a character name.
		if trimmed == strings.ToUpper(trimmed) && !strings.Contains(trimmed, ":") && len(trimmed) > 1 {
			flush()
			currentChar = trimmed
			continue
		}

		// Anything else while we have a character is dialogue.
		if currentChar != "" {
			if dialogueBuf.Len() > 0 {
				dialogueBuf.WriteByte(' ')
			}
			dialogueBuf.WriteString(trimmed)
		}
	}
	flush()
	return turns
}
