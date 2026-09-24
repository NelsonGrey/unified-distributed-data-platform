package auth

import (
	"context"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func handlerCalled(t *testing.T) (grpc.UnaryHandler, *bool) {
	t.Helper()
	called := false
	return func(ctx context.Context, req any) (any, error) {
		called = true
		return "ok", nil
	}, &called
}

func TestUnaryServerInterceptorRejectsMissingMetadata(t *testing.T) {
	interceptor := UnaryServerInterceptor("secret")
	handler, called := handlerCalled(t)

	_, err := interceptor(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/uddp.native.v1.StateService/Get"}, handler)
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("expected Unauthenticated, got %v", err)
	}
	if *called {
		t.Fatal("handler must not run when auth fails")
	}
}

func TestUnaryServerInterceptorRejectsWrongToken(t *testing.T) {
	interceptor := UnaryServerInterceptor("secret")
	handler, called := handlerCalled(t)

	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer wrong"))
	_, err := interceptor(ctx, nil, &grpc.UnaryServerInfo{FullMethod: "/uddp.native.v1.StateService/Get"}, handler)
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("expected Unauthenticated, got %v", err)
	}
	if *called {
		t.Fatal("handler must not run when auth fails")
	}
}

func TestUnaryServerInterceptorAcceptsCorrectToken(t *testing.T) {
	interceptor := UnaryServerInterceptor("secret")
	handler, called := handlerCalled(t)

	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer secret"))
	resp, err := interceptor(ctx, nil, &grpc.UnaryServerInfo{FullMethod: "/uddp.native.v1.StateService/Get"}, handler)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if resp != "ok" || !*called {
		t.Fatal("expected handler to run and its response to be returned")
	}
}

func TestUnaryServerInterceptorAcceptsTokenWithoutBearerPrefix(t *testing.T) {
	interceptor := UnaryServerInterceptor("secret")
	handler, _ := handlerCalled(t)

	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "secret"))
	if _, err := interceptor(ctx, nil, &grpc.UnaryServerInfo{FullMethod: "/uddp.native.v1.StateService/Get"}, handler); err != nil {
		t.Fatalf("expected bare token (no Bearer prefix) to be accepted, got %v", err)
	}
}

func TestUnaryServerInterceptorExemptsHealthChecks(t *testing.T) {
	interceptor := UnaryServerInterceptor("secret")
	handler, called := handlerCalled(t)

	_, err := interceptor(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/grpc.health.v1.Health/Check"}, handler)
	if err != nil {
		t.Fatalf("expected health check to bypass auth, got %v", err)
	}
	if !*called {
		t.Fatal("expected handler to run for health check")
	}
}
