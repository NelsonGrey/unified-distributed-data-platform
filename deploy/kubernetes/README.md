# Kubernetes deployment (single-node)

Scoped to delivery slice 1/1.5: one `uddp-node` pod, `cache` profile only. There is no replication yet, so `replicas` must stay at 1 — this is not a highly-available deployment.

## Deploy

```sh
docker build -t <your-registry>/uddp-node:<tag> .
docker push <your-registry>/uddp-node:<tag>
```

Edit `statefulset.yaml`'s `image:` to point at the pushed tag (it defaults to `uddp-node:local`, a locally-built image, for local `kind`/`minikube`/Docker Desktop testing), then:

```sh
kubectl apply -k deploy/kubernetes
```

## What's here

- `statefulset.yaml` — one replica, a per-pod PVC for the WAL (`/data`), readiness/liveness probes against `/healthz`, `fsGroup` set to match the container's distroless `nonroot` UID/GID (65532) so the PVC mount is writable.
- `service.yaml` — headless (no ClusterIP/load-balancing, since there's exactly one backend); clients address the pod via `uddp-node-0.uddp-node.<namespace>.svc.cluster.local:7070`.

## Not here yet

- Multi-replica / StatefulSet scaling — meaningless until delivery slice 2 (replication) ships; a second replica today would just be a second, independent, non-replicated partition.
- NetworkPolicy, PodDisruptionBudget, resource-based HPA, and a Helm chart — deferred until there's a second deployment target to generalize from.
- TLS/mTLS on the gRPC listener (TRD §7) — currently plaintext; fine for local/trusted-cluster use, not for anything crossing a trust boundary.
