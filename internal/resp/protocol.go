// Package resp implements a declared, minimal subset of the Redis RESP2
// protocol (BR-006/TR-009 — "protocol adapters implement declared subsets
// only," TRD 4.1): PING, GET, SET (with optional EX), and DEL. This is
// meant to be the starting point for a real compatibility matrix
// (docs/REDIS_COMPATIBILITY.md), not a claim of Redis compatibility itself
// — anything outside the declared subset returns an explicit RESP error
// (TR-010), never a best-effort approximation.
//
// Explicitly deferred: RESP3, pipelining beyond what a single connection's
// sequential read loop gives for free, transactions (MULTI/EXEC),
// pub/sub, and every other Redis command. TLS/auth on this listener are
// also not wired in yet — see server.go.
package resp

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
)

// Command is one parsed RESP request: a command name and its arguments,
// both already unwrapped from RESP bulk strings.
type Command struct {
	Name string
	Args [][]byte
}

// ReadCommand reads one RESP request (a RESP array of bulk strings, the
// form every real Redis client sends for commands) from r.
func ReadCommand(r *bufio.Reader) (Command, error) {
	line, err := readLine(r)
	if err != nil {
		return Command{}, err
	}
	if len(line) == 0 || line[0] != '*' {
		return Command{}, fmt.Errorf("resp: expected array (*), got %q", line)
	}
	n, err := strconv.Atoi(string(line[1:]))
	if err != nil || n < 1 {
		return Command{}, fmt.Errorf("resp: invalid array length %q", line[1:])
	}

	parts := make([][]byte, n)
	for i := 0; i < n; i++ {
		b, err := readBulkString(r)
		if err != nil {
			return Command{}, err
		}
		parts[i] = b
	}

	return Command{Name: string(parts[0]), Args: parts[1:]}, nil
}

func readBulkString(r *bufio.Reader) ([]byte, error) {
	line, err := readLine(r)
	if err != nil {
		return nil, err
	}
	if len(line) == 0 || line[0] != '$' {
		return nil, fmt.Errorf("resp: expected bulk string ($), got %q", line)
	}
	n, err := strconv.Atoi(string(line[1:]))
	if err != nil {
		return nil, fmt.Errorf("resp: invalid bulk string length %q", line[1:])
	}
	if n < 0 {
		return nil, nil // null bulk string
	}

	buf := make([]byte, n+2) // +2 for trailing \r\n
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	return buf[:n], nil
}

func readLine(r *bufio.Reader) ([]byte, error) {
	line, err := r.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	// Trim trailing \r\n (or just \n if a client is sloppy about it).
	line = line[:len(line)-1]
	if len(line) > 0 && line[len(line)-1] == '\r' {
		line = line[:len(line)-1]
	}
	return line, nil
}

// Reply writers. Each writes one complete RESP reply.

func WriteSimpleString(w io.Writer, s string) error {
	_, err := fmt.Fprintf(w, "+%s\r\n", s)
	return err
}

func WriteError(w io.Writer, msg string) error {
	_, err := fmt.Fprintf(w, "-ERR %s\r\n", msg)
	return err
}

func WriteInteger(w io.Writer, n int64) error {
	_, err := fmt.Fprintf(w, ":%d\r\n", n)
	return err
}

func WriteBulkString(w io.Writer, b []byte) error {
	if _, err := fmt.Fprintf(w, "$%d\r\n", len(b)); err != nil {
		return err
	}
	if _, err := w.Write(b); err != nil {
		return err
	}
	_, err := w.Write([]byte("\r\n"))
	return err
}

func WriteNullBulkString(w io.Writer) error {
	_, err := w.Write([]byte("$-1\r\n"))
	return err
}
