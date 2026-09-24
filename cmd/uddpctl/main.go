// Command uddpctl is the local-development CLI (TRD 4.2 "control plane, CLI"):
// point it at a running uddp-node to exercise the KV surface
// (get/put/delete) and the change stream it produces atomically
// (fetch/commit-offset/offset), demonstrating BR-003's no-dual-write claim
// end to end.
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"os"
	"strconv"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	adminv1 "github.com/marknelson/uddp/api/admin/v1"
	nativev1 "github.com/marknelson/uddp/api/native/v1"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "uddpctl:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	fs := flag.NewFlagSet("uddpctl", flag.ExitOnError)
	addr := fs.String("addr", "127.0.0.1:7070", "uddp-node gRPC address")
	namespace := fs.String("namespace", "default", "namespace id")
	useTLS := fs.Bool("tls", false, "use TLS to connect (implied by --tls-ca or --insecure-skip-verify)")
	tlsCA := fs.String("tls-ca", "", "path to a PEM CA certificate to trust, in addition to the system roots (for a self-signed or private CA)")
	insecureSkipVerify := fs.Bool("insecure-skip-verify", false, "skip server certificate verification (dev only, e.g. against a self-signed cert with no --tls-ca)")
	token := fs.String("token", "", "bearer token to send with every request (prefer the UDDP_TOKEN env var over this flag, which is visible in the process list)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	rest := fs.Args()
	if len(rest) == 0 {
		return usageError()
	}

	if *token == "" {
		*token = os.Getenv("UDDP_TOKEN")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if *token != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+*token)
	}

	transportCreds, err := dialCredentials(*useTLS, *tlsCA, *insecureSkipVerify)
	if err != nil {
		return err
	}

	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(transportCreds))
	if err != nil {
		return fmt.Errorf("dial %s: %w", *addr, err)
	}
	defer conn.Close()
	client := nativev1.NewStateServiceClient(conn)
	streamClient := nativev1.NewStreamServiceClient(conn)
	adminClient := adminv1.NewAdminServiceClient(conn)

	switch cmd, rest := rest[0], rest[1:]; cmd {
	case "get":
		if len(rest) != 1 {
			return fmt.Errorf("usage: uddpctl get <key>")
		}
		resp, err := client.Get(ctx, &nativev1.GetRequest{NamespaceId: *namespace, Key: []byte(rest[0])})
		if err != nil {
			return err
		}
		if !resp.Found {
			fmt.Println("(not found)")
			return nil
		}
		fmt.Printf("%s\t(version=%d)\n", resp.Value, resp.Version)
		return nil

	case "put":
		if len(rest) != 2 {
			return fmt.Errorf("usage: uddpctl put <key> <value>")
		}
		resp, err := client.Put(ctx, &nativev1.PutRequest{NamespaceId: *namespace, Key: []byte(rest[0]), Value: []byte(rest[1])})
		if err != nil {
			return err
		}
		fmt.Printf("committed at position %d (profile=%s)\n", resp.CommitPosition, resp.DurabilityProfile)
		return nil

	case "delete":
		if len(rest) != 1 {
			return fmt.Errorf("usage: uddpctl delete <key>")
		}
		resp, err := client.Delete(ctx, &nativev1.DeleteRequest{NamespaceId: *namespace, Key: []byte(rest[0])})
		if err != nil {
			return err
		}
		fmt.Printf("committed at position %d\n", resp.CommitPosition)
		return nil

	case "fetch":
		if len(rest) < 1 || len(rest) > 2 {
			return fmt.Errorf("usage: uddpctl fetch <from-offset> [max-records]")
		}
		from, err := strconv.ParseUint(rest[0], 10, 64)
		if err != nil {
			return fmt.Errorf("invalid from-offset %q: %w", rest[0], err)
		}
		var maxRecords int32 = 100
		if len(rest) == 2 {
			n, err := strconv.ParseInt(rest[1], 10, 32)
			if err != nil {
				return fmt.Errorf("invalid max-records %q: %w", rest[1], err)
			}
			maxRecords = int32(n)
		}
		resp, err := streamClient.Fetch(ctx, &nativev1.FetchRequest{NamespaceId: *namespace, FromOffset: from, MaxRecords: maxRecords})
		if err != nil {
			return err
		}
		for _, r := range resp.Records {
			fmt.Printf("%d\t%s\t%s=%s\n", r.Offset, r.Kind, r.Key, r.Value)
		}
		fmt.Printf("next_offset=%d\n", resp.NextOffset)
		return nil

	case "commit-offset":
		if len(rest) != 2 {
			return fmt.Errorf("usage: uddpctl commit-offset <group> <offset>")
		}
		offset, err := strconv.ParseUint(rest[1], 10, 64)
		if err != nil {
			return fmt.Errorf("invalid offset %q: %w", rest[1], err)
		}
		resp, err := streamClient.CommitOffset(ctx, &nativev1.CommitOffsetRequest{NamespaceId: *namespace, ConsumerGroup: rest[0], Offset: offset})
		if err != nil {
			return err
		}
		fmt.Printf("committed_offset=%d\n", resp.CommittedOffset)
		return nil

	case "offset":
		if len(rest) != 1 {
			return fmt.Errorf("usage: uddpctl offset <group>")
		}
		resp, err := streamClient.FetchOffset(ctx, &nativev1.FetchOffsetRequest{NamespaceId: *namespace, ConsumerGroup: rest[0]})
		if err != nil {
			return err
		}
		if !resp.Found {
			fmt.Println("(no committed offset)")
			return nil
		}
		fmt.Printf("offset=%d\n", resp.Offset)
		return nil

	case "add-node":
		if len(rest) != 2 {
			return fmt.Errorf("usage: uddpctl add-node <id> <raft-addr>")
		}
		resp, err := adminClient.AddNode(ctx, &adminv1.AddNodeRequest{Id: rest[0], RaftAddr: rest[1]})
		if err != nil {
			return err
		}
		printCluster(resp.Cluster)
		return nil

	case "remove-node":
		if len(rest) != 1 {
			return fmt.Errorf("usage: uddpctl remove-node <id>")
		}
		resp, err := adminClient.RemoveNode(ctx, &adminv1.RemoveNodeRequest{Id: rest[0]})
		if err != nil {
			return err
		}
		printCluster(resp.Cluster)
		return nil

	case "list-nodes":
		if len(rest) != 0 {
			return fmt.Errorf("usage: uddpctl list-nodes")
		}
		resp, err := adminClient.ListNodes(ctx, &adminv1.ListNodesRequest{})
		if err != nil {
			return err
		}
		printCluster(resp.Cluster)
		return nil

	default:
		return usageError()
	}
}

func printCluster(c *adminv1.Cluster) {
	fmt.Printf("leader: %s (%s)\n", c.LeaderId, c.LeaderRaftAddr)
	for _, n := range c.Nodes {
		voter := ""
		if n.IsVoter {
			voter = " voter"
		}
		fmt.Printf("  %s\t%s%s\n", n.Id, n.RaftAddr, voter)
	}
}

func usageError() error {
	return fmt.Errorf("usage: uddpctl [--addr host:port] [--namespace id] [--tls] [--tls-ca file] [--insecure-skip-verify] [--token t] <get|put|delete|fetch|commit-offset|offset|add-node|remove-node|list-nodes> ...")
}

func dialCredentials(useTLS bool, caPath string, insecureSkipVerify bool) (credentials.TransportCredentials, error) {
	if !useTLS && caPath == "" && !insecureSkipVerify {
		return insecure.NewCredentials(), nil
	}

	cfg := &tls.Config{InsecureSkipVerify: insecureSkipVerify}
	if caPath != "" {
		pem, err := os.ReadFile(caPath)
		if err != nil {
			return nil, fmt.Errorf("read --tls-ca %s: %w", caPath, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("no valid certificates found in --tls-ca %s", caPath)
		}
		cfg.RootCAs = pool
	}
	return credentials.NewTLS(cfg), nil
}
