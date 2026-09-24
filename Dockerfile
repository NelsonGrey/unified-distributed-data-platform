FROM golang:1.25 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/uddp-node ./cmd/uddp-node
RUN CGO_ENABLED=0 go build -o /out/uddpctl ./cmd/uddpctl
RUN mkdir -p /out/data

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/uddp-node /out/uddpctl /usr/local/bin/
# Pre-create /data owned by the nonroot user this image runs as, since
# distroless has no shell to chown at runtime. A Kubernetes PVC mounted at
# /data still needs its own ownership handled via the pod's securityContext
# (fsGroup) — this only covers `docker run` / local use without a PVC.
COPY --from=build --chown=nonroot:nonroot /out/data /data
USER nonroot:nonroot
EXPOSE 7070 7071
ENTRYPOINT ["/usr/local/bin/uddp-node"]
CMD ["--data-dir=/data", "--addr=0.0.0.0:7070", "--http-addr=0.0.0.0:7071"]
