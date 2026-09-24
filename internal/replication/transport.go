package replication

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/hashicorp/raft"
)

// tlsStreamLayer implements raft.StreamLayer over mutual TLS: every raft
// RPC between nodes (AppendEntries, RequestVote, InstallSnapshot) is
// authenticated in both directions and encrypted in transit. This is the
// real mTLS workload identity TRD §7 asks for — unlike the client-facing
// gRPC API's bearer token (internal/auth), which is an explicitly smaller
// stand-in, raft-to-raft traffic is exactly the workload-to-workload case
// mTLS is for for: a small, fixed set of known peers who can hold real
// certificates, not arbitrary external clients.
type tlsStreamLayer struct {
	net.Listener
	dialTLSConfig *tls.Config
}

func (t *tlsStreamLayer) Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	d := &net.Dialer{Timeout: timeout}
	return tls.DialWithDialer(d, "tcp", string(address), t.dialTLSConfig)
}

// TLSConfig configures mutual TLS for the raft transport. All three
// fields are required together — there's no partial/one-way mode, since
// an unauthenticated peer being able to send AppendEntries/RequestVote is
// exactly the "confused deputy"/stale-writer risk TRD §7's threat model
// calls out.
type TLSConfig struct {
	CertFile   string // this node's certificate
	KeyFile    string // this node's private key
	PeerCAFile string // CA that signed every node's certificate, used to verify peers in both directions
}

func newTLSStreamLayer(bindAddr string, cfg TLSConfig) (*tlsStreamLayer, error) {
	cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("replication: load raft TLS keypair: %w", err)
	}

	caPEM, err := os.ReadFile(cfg.PeerCAFile)
	if err != nil {
		return nil, fmt.Errorf("replication: read raft peer CA %s: %w", cfg.PeerCAFile, err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("replication: no valid certificates found in raft peer CA %s", cfg.PeerCAFile)
	}

	serverTLSConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientCAs:    pool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
	}
	listener, err := tls.Listen("tcp", bindAddr, serverTLSConfig)
	if err != nil {
		return nil, fmt.Errorf("replication: listen (tls) on %s: %w", bindAddr, err)
	}

	dialTLSConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      pool,
	}

	return &tlsStreamLayer{Listener: listener, dialTLSConfig: dialTLSConfig}, nil
}
