package replication

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hashicorp/raft"

	"github.com/marknelson/uddp/internal/engine"
)

func openTestEngine(t *testing.T) (*engine.Engine, error) {
	t.Helper()
	return engine.Open(filepath.Join(t.TempDir(), "engine.wal"), nil)
}

// testCA generates a CA and returns a function that mints a leaf
// certificate (signed by that CA) for a given hostname/IP, plus the CA's
// own PEM bytes. Kept local to the test rather than shelling out to
// openssl (as the README's manual example does) so the test has no
// external tool dependency.
type testCA struct {
	certPEM []byte
	key     *ecdsa.PrivateKey
	cert    *x509.Certificate
}

func newTestCA(t *testing.T) *testCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate CA key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create CA cert: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse CA cert: %v", err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	return &testCA{certPEM: certPEM, key: key, cert: cert}
}

// issue writes a leaf cert+key (PEM) to certPath/keyPath, valid for
// 127.0.0.1, signed by ca.
func (ca *testCA) issue(t *testing.T, certPath, keyPath string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate leaf key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: "raft-node"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatalf("create leaf cert: %v", err)
	}
	certOut, err := os.Create(certPath)
	if err != nil {
		t.Fatalf("create %s: %v", certPath, err)
	}
	defer certOut.Close()
	if err := pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: der}); err != nil {
		t.Fatalf("write cert: %v", err)
	}

	keyBytes, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal leaf key: %v", err)
	}
	keyOut, err := os.Create(keyPath)
	if err != nil {
		t.Fatalf("create %s: %v", keyPath, err)
	}
	defer keyOut.Close()
	if err := pem.Encode(keyOut, &pem.Block{Type: "EC PRIVATE KEY", Bytes: keyBytes}); err != nil {
		t.Fatalf("write key: %v", err)
	}
}

func TestClusterOverMutualTLS(t *testing.T) {
	ca := newTestCA(t)
	dir := t.TempDir()
	caPath := filepath.Join(dir, "ca.pem")
	if err := os.WriteFile(caPath, ca.certPEM, 0o644); err != nil {
		t.Fatalf("write CA: %v", err)
	}

	peers := make([]Peer, 3)
	tlsCfgs := make([]*TLSConfig, 3)
	for i := 0; i < 3; i++ {
		peers[i] = Peer{ID: peerID(i), Addr: freeAddr(t)}
		certPath := filepath.Join(dir, peerID(i)+"-cert.pem")
		keyPath := filepath.Join(dir, peerID(i)+"-key.pem")
		ca.issue(t, certPath, keyPath)
		tlsCfgs[i] = &TLSConfig{CertFile: certPath, KeyFile: keyPath, PeerCAFile: caPath}
	}

	nodes := make([]testNode, 3)
	for i, p := range peers {
		eng, err := openTestEngine(t)
		if err != nil {
			t.Fatalf("open engine: %v", err)
		}
		node, err := Open(eng, Config{
			ID: p.ID, BindAddr: p.Addr, DataDir: t.TempDir(),
			Bootstrap: i == 0, Peers: peers, ApplyTimeout: 5 * time.Second,
			TLS: tlsCfgs[i],
		})
		if err != nil {
			t.Fatalf("open node %s over TLS: %v", p.ID, err)
		}
		t.Cleanup(func() { node.Shutdown() })
		nodes[i] = testNode{node: node, engine: eng}
	}

	leader := waitForLeader(t, nodes)
	result, err := leader.node.Propose(Command{Op: OpPut, Key: []byte("k"), Value: []byte("v")})
	if err != nil {
		t.Fatalf("propose over mTLS: %v", err)
	}
	if result.Err != nil {
		t.Fatalf("apply result: %v", result.Err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for {
		ok := true
		for i := range nodes {
			if _, _, found := nodes[i].engine.Get([]byte("k")); !found {
				ok = false
			}
		}
		if ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("replicas did not converge over mTLS transport within timeout")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestTLSStreamLayerRejectsConnectionSignedByDifferentCA(t *testing.T) {
	trustedCA := newTestCA(t)
	untrustedCA := newTestCA(t)
	dir := t.TempDir()

	trustedCAPath := filepath.Join(dir, "trusted-ca.pem")
	if err := os.WriteFile(trustedCAPath, trustedCA.certPEM, 0o644); err != nil {
		t.Fatalf("write trusted CA: %v", err)
	}

	serverCert := filepath.Join(dir, "server-cert.pem")
	serverKey := filepath.Join(dir, "server-key.pem")
	trustedCA.issue(t, serverCert, serverKey)

	serverAddr := freeAddr(t)
	server, err := newTLSStreamLayer(serverAddr, TLSConfig{CertFile: serverCert, KeyFile: serverKey, PeerCAFile: trustedCAPath})
	if err != nil {
		t.Fatalf("start TLS listener: %v", err)
	}
	defer server.Close()

	acceptErr := make(chan error, 1)
	go func() {
		conn, err := server.Accept()
		if err != nil {
			acceptErr <- err
			return
		}
		defer conn.Close()
		// Accept() returns before the TLS handshake completes — it's
		// deferred to first I/O — so an untrusted client cert only
		// surfaces here, not at Accept() itself.
		_, err = conn.Read(make([]byte, 1))
		acceptErr <- err
	}()

	// Client presents a cert signed by a DIFFERENT CA than the server
	// trusts — the server must reject the handshake (this is the actual
	// fencing property: an untrusted node cannot join the raft cluster's
	// traffic even if it knows the address).
	untrustedClientCert := filepath.Join(dir, "untrusted-cert.pem")
	untrustedClientKey := filepath.Join(dir, "untrusted-key.pem")
	untrustedCA.issue(t, untrustedClientCert, untrustedClientKey)
	untrustedCAPath := filepath.Join(dir, "untrusted-ca.pem")
	if err := os.WriteFile(untrustedCAPath, untrustedCA.certPEM, 0o644); err != nil {
		t.Fatalf("write untrusted CA: %v", err)
	}

	client, err := newTLSStreamLayer(freeAddr(t), TLSConfig{CertFile: untrustedClientCert, KeyFile: untrustedClientKey, PeerCAFile: untrustedCAPath})
	if err != nil {
		t.Fatalf("build client stream layer: %v", err)
	}
	defer client.Close()

	_, dialErr := client.Dial(raft.ServerAddress(serverAddr), 2*time.Second)
	if dialErr == nil {
		t.Fatal("expected Dial with an untrusted client cert to fail")
	}

	select {
	case err := <-acceptErr:
		if err == nil {
			t.Fatal("expected the server's Accept-side handshake to fail for an untrusted client cert")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server Accept did not return within timeout")
	}
}

func peerID(i int) string {
	return string(rune('a' + i))
}
