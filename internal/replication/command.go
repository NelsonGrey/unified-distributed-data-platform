// Package replication drives the engine as a Raft-replicated state
// machine (delivery slice 2: "partition replication, metadata consensus,
// fencing"), using hashicorp/raft rather than a hand-rolled consensus
// protocol — correctness of the consensus algorithm itself is not
// something to reinvent for a system whose core promise is "no
// acknowledged write loss."
//
// Scope explicitly deferred in this increment (documented here, not
// hidden): log compaction/snapshot-triggered catch-up (nodes always catch
// up via full log replay — see fsm.go), multi-partition placement, and the
// control-plane's own consensus (this package only replicates the data
// plane's single partition).
package replication

import (
	"bytes"
	"encoding/gob"
	"fmt"
)

// Op identifies which engine mutation a Command applies.
type Op uint8

const (
	OpPut Op = iota + 1
	OpDelete
	OpCompareAndSet
)

// Command is what gets proposed to Raft and replicated to every node.
// ExpiresAtUnixNano is resolved once by the proposer (see Node.Propose)
// rather than recomputed by each replica at apply time — see
// engine.ApplyPut for why that determinism matters.
type Command struct {
	Op                Op
	Key               []byte
	Value             []byte
	ExpiresAtUnixNano int64
	ExpectedVersion   uint64 // OpCompareAndSet only
	IdempotencyKey    string
}

func encodeCommand(cmd Command) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(cmd); err != nil {
		return nil, fmt.Errorf("replication: encode command: %w", err)
	}
	return buf.Bytes(), nil
}

func decodeCommand(b []byte) (Command, error) {
	var cmd Command
	if err := gob.NewDecoder(bytes.NewReader(b)).Decode(&cmd); err != nil {
		return Command{}, fmt.Errorf("replication: decode command: %w", err)
	}
	return cmd, nil
}
