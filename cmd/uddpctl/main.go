// Command uddpctl is the local-development CLI (TRD 4.2 "control plane, CLI")
// scoped to slice 1: point it at a running uddp-node and get/put/delete/cas
// keys.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

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
	if err := fs.Parse(args); err != nil {
		return err
	}
	rest := fs.Args()
	if len(rest) == 0 {
		return usageError()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial %s: %w", *addr, err)
	}
	defer conn.Close()
	client := nativev1.NewStateServiceClient(conn)

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

	default:
		return usageError()
	}
}

func usageError() error {
	return fmt.Errorf("usage: uddpctl [--addr host:port] [--namespace id] <get|put|delete> ...")
}
