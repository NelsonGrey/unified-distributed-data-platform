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

## Enabling TLS and authentication

`uddp-node` supports both (`--tls-cert`/`--tls-key`, `--auth-token`/`UDDP_AUTH_TOKEN`), but the manifests here don't wire them in yet — that needs a real certificate (cert-manager or otherwise) and a Secret for the token, which are deployment-specific. In the meantime, add to the container spec in `statefulset.yaml`:

```yaml
env:
  - name: UDDP_AUTH_TOKEN
    valueFrom:
      secretKeyRef:
        name: uddp-node-auth
        key: token
volumeMounts:
  - name: tls
    mountPath: /tls
    readOnly: true
volumes:
  - name: tls
    secret:
      secretName: uddp-node-tls
```

and add `--tls-cert=/tls/tls.crt --tls-key=/tls/tls.key` to `args`. Until this is wired in, treat the cluster's own network boundary (NetworkPolicy — not yet added either, see below) as the only isolation this deployment has.

## Not here yet

- Multi-replica / StatefulSet scaling — meaningless until delivery slice 2 (replication) ships; a second replica today would just be a second, independent, non-replicated partition.
- NetworkPolicy, PodDisruptionBudget, resource-based HPA, and a Helm chart — deferred until there's a second deployment target to generalize from.
- Wiring TLS/auth into the manifests by default (see above) — supported by the binary, not yet defaulted here since it needs a cert source and a Secret this repo can't provide for you.
- mTLS / OIDC workload identity (TRD §7) — the shared bearer token is a smaller stand-in, not that mechanism.
