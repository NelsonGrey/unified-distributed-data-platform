// Package auth implements the minimal authentication slice this build
// needs: a shared bearer token checked on every RPC. TRD 7 specifies mTLS
// and/or short-lived OIDC tokens as the real workload-identity mechanism;
// a static shared token is a deliberately smaller stand-in so a deployment
// isn't left with "authentication required" unmet while mTLS/OIDC
// federation (which needs a CA or identity provider integration) is staged
// for a later slice. It is not a substitute for that work — see TRD 7 and
// SECURITY.md.
package auth

import (
	"context"
	"crypto/subtle"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const metadataKey = "authorization"

// bearerPrefix is stripped from the "authorization" metadata value, so
// clients send the conventional "Bearer <token>" form.
const bearerPrefix = "Bearer "

// UnaryServerInterceptor rejects any RPC that doesn't present token via
// gRPC metadata as "authorization: Bearer <token>". Comparison is
// constant-time to avoid leaking the token through response-time
// differences. Health checks are exempt so orchestrators (kubelet, load
// balancers) can probe liveness/readiness without holding a credential.
func UnaryServerInterceptor(token string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if info.FullMethod == "/grpc.health.v1.Health/Check" || info.FullMethod == "/grpc.health.v1.Health/Watch" {
			return handler(ctx, req)
		}

		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			return nil, status.Error(codes.Unauthenticated, "missing authorization metadata")
		}
		values := md.Get(metadataKey)
		if len(values) == 0 {
			return nil, status.Error(codes.Unauthenticated, "missing authorization metadata")
		}

		presented := values[0]
		if len(presented) > len(bearerPrefix) && presented[:len(bearerPrefix)] == bearerPrefix {
			presented = presented[len(bearerPrefix):]
		}

		if subtle.ConstantTimeCompare([]byte(presented), []byte(token)) != 1 {
			return nil, status.Error(codes.Unauthenticated, "invalid token")
		}

		return handler(ctx, req)
	}
}
