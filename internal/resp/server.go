package resp

import (
	"bufio"
	"errors"
	"io"
	"log"
	"net"
	"strconv"
	"strings"

	"github.com/marknelson/uddp/internal/engine"
)

// Server serves the declared RESP subset over a single engine. Unlike the
// native gRPC API, it has no TLS/auth/namespace-routing wired in yet —
// this is a compatibility on-ramp, not a hardened listener; treat it as
// trusted-network-only until that catches up (see the package doc).
type Server struct {
	Engine *engine.Engine
}

// ListenAndServe accepts connections on addr until the listener is closed
// or an unrecoverable error occurs.
func (s *Server) ListenAndServe(addr string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	defer lis.Close()

	for {
		conn, err := lis.Accept()
		if err != nil {
			return err
		}
		go s.handleConn(conn)
	}
}

func (s *Server) handleConn(conn net.Conn) {
	defer conn.Close()
	r := bufio.NewReader(conn)

	for {
		cmd, err := ReadCommand(r)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				log.Printf("resp: connection error: %v", err)
			}
			return
		}
		if err := s.dispatch(conn, cmd); err != nil {
			log.Printf("resp: write error: %v", err)
			return
		}
	}
}

// dispatch handles one command, writing exactly one RESP reply.
// Unsupported commands get an explicit error, never a silent
// approximation (TR-010) — this is the whole point of a "declared
// subset" rather than a best-effort passthrough.
func (s *Server) dispatch(w io.Writer, cmd Command) error {
	switch strings.ToUpper(cmd.Name) {
	case "PING":
		return WriteSimpleString(w, "PONG")

	case "GET":
		if len(cmd.Args) != 1 {
			return WriteError(w, "wrong number of arguments for 'get' command")
		}
		value, _, found := s.Engine.Get(cmd.Args[0])
		if !found {
			return WriteNullBulkString(w)
		}
		return WriteBulkString(w, value)

	case "SET":
		if len(cmd.Args) < 2 {
			return WriteError(w, "wrong number of arguments for 'set' command")
		}
		key, value := cmd.Args[0], cmd.Args[1]
		var ttlSeconds int64
		if len(cmd.Args) >= 4 && strings.EqualFold(string(cmd.Args[2]), "EX") {
			n, err := strconv.ParseInt(string(cmd.Args[3]), 10, 64)
			if err != nil || n <= 0 {
				return WriteError(w, "invalid expire time in 'set' command")
			}
			ttlSeconds = n
		} else if len(cmd.Args) > 2 {
			return WriteError(w, "unsupported SET option (only EX is implemented)")
		}
		if _, err := s.Engine.Put(key, value, ttlSeconds, ""); err != nil {
			return WriteError(w, err.Error())
		}
		return WriteSimpleString(w, "OK")

	case "DEL":
		if len(cmd.Args) < 1 {
			return WriteError(w, "wrong number of arguments for 'del' command")
		}
		var deleted int64
		for _, key := range cmd.Args {
			_, _, found := s.Engine.Get(key)
			if !found {
				continue
			}
			if _, err := s.Engine.Delete(key, ""); err != nil {
				return WriteError(w, err.Error())
			}
			deleted++
		}
		return WriteInteger(w, deleted)

	default:
		return WriteError(w, "unknown command '"+cmd.Name+"' (unsupported: this adapter implements a declared subset only — see docs/REDIS_COMPATIBILITY.md)")
	}
}
