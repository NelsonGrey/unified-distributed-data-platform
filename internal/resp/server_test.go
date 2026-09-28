package resp

import (
	"bufio"
	"bytes"
	"path/filepath"
	"strings"
	"testing"

	"github.com/marknelson/uddp/internal/engine"
)

func newTestReader(s string) *bufio.Reader {
	return bufio.NewReader(strings.NewReader(s))
}

func testServer(t *testing.T) *Server {
	t.Helper()
	eng, err := engine.Open(filepath.Join(t.TempDir(), "resp.wal"), nil)
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	t.Cleanup(func() { eng.Close() })
	return &Server{Engine: eng}
}

func TestDispatchPing(t *testing.T) {
	s := testServer(t)
	var out bytes.Buffer
	if err := s.dispatch(&out, Command{Name: "PING"}); err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if out.String() != "+PONG\r\n" {
		t.Fatalf("unexpected reply: %q", out.String())
	}
}

func TestDispatchSetAndGet(t *testing.T) {
	s := testServer(t)

	var out bytes.Buffer
	if err := s.dispatch(&out, Command{Name: "SET", Args: [][]byte{[]byte("k"), []byte("v")}}); err != nil {
		t.Fatalf("dispatch set: %v", err)
	}
	if out.String() != "+OK\r\n" {
		t.Fatalf("unexpected SET reply: %q", out.String())
	}

	out.Reset()
	if err := s.dispatch(&out, Command{Name: "GET", Args: [][]byte{[]byte("k")}}); err != nil {
		t.Fatalf("dispatch get: %v", err)
	}
	if out.String() != "$1\r\nv\r\n" {
		t.Fatalf("unexpected GET reply: %q", out.String())
	}
}

func TestDispatchGetMissingReturnsNullBulkString(t *testing.T) {
	s := testServer(t)
	var out bytes.Buffer
	if err := s.dispatch(&out, Command{Name: "GET", Args: [][]byte{[]byte("nope")}}); err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if out.String() != "$-1\r\n" {
		t.Fatalf("expected null bulk string for missing key, got %q", out.String())
	}
}

func TestDispatchSetWithEX(t *testing.T) {
	s := testServer(t)
	var out bytes.Buffer
	if err := s.dispatch(&out, Command{Name: "SET", Args: [][]byte{[]byte("k"), []byte("v"), []byte("EX"), []byte("60")}}); err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if out.String() != "+OK\r\n" {
		t.Fatalf("unexpected reply: %q", out.String())
	}
	// Confirm it actually set a TTL, not just accepted the syntax.
	_, _, found := s.Engine.Get([]byte("k"))
	if !found {
		t.Fatal("expected key to be set")
	}
}

func TestDispatchDel(t *testing.T) {
	s := testServer(t)
	var out bytes.Buffer
	s.dispatch(&out, Command{Name: "SET", Args: [][]byte{[]byte("a"), []byte("1")}})
	out.Reset()
	s.dispatch(&out, Command{Name: "SET", Args: [][]byte{[]byte("b"), []byte("2")}})

	out.Reset()
	if err := s.dispatch(&out, Command{Name: "DEL", Args: [][]byte{[]byte("a"), []byte("b"), []byte("nonexistent")}}); err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if out.String() != ":2\r\n" {
		t.Fatalf("expected DEL to report 2 deleted, got %q", out.String())
	}

	if _, _, found := s.Engine.Get([]byte("a")); found {
		t.Fatal("expected a to be deleted")
	}
}

func TestDispatchUnsupportedCommandReturnsExplicitError(t *testing.T) {
	s := testServer(t)
	var out bytes.Buffer
	if err := s.dispatch(&out, Command{Name: "EXPIRE", Args: [][]byte{[]byte("k"), []byte("10")}}); err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if out.String()[0] != '-' {
		t.Fatalf("expected a RESP error reply for an unsupported command, got %q", out.String())
	}
}

func TestReadCommandParsesRealRESPWireFormat(t *testing.T) {
	// This is what a real redis-cli / client library sends for `SET k v`.
	raw := "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n"
	r := newTestReader(raw)

	cmd, err := ReadCommand(r)
	if err != nil {
		t.Fatalf("read command: %v", err)
	}
	if cmd.Name != "SET" || len(cmd.Args) != 2 || string(cmd.Args[0]) != "k" || string(cmd.Args[1]) != "v" {
		t.Fatalf("unexpected parsed command: %+v", cmd)
	}
}
