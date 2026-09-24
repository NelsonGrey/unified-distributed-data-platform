# Contributing

Unified Distributed Data Platform is closed-source. The source is publicly visible, but this isn't an open-source project — there's no public issue tracker and outside pull requests aren't accepted.

If you have collaborator access to this repository:

1. Branch from `develop` (`feature/<short-description>` or `fix/<short-description>`) — `develop` is the active integration branch; `staging` and `main` are only updated via promotion PRs.
2. Keep commits focused, and write commit messages that explain *why*, not just *what*.
3. Before opening a pull request, run `go build ./...`, `go vet ./...`, and `go test -race ./...`, and make sure `gofmt -l .` reports no files. CI runs the same checks plus coverage (`-cover`).
4. Add a unit test for new behavior in the same PR — this codebase doesn't accumulate untested surface area and let a test backlog build up later. Hot paths (WAL append/read, engine mutations, offset commits) should get a `testing.B` benchmark alongside their tests; see `internal/wal/wal_bench_test.go` for the pattern. CI runs `go test -bench=. -benchmem ./...` on every PR as an informational, non-blocking regression tripwire — it's there to catch an accidental 10x slowdown, not to back a performance claim (that needs the full TR-019 harness, which doesn't exist yet).
5. Open the PR against `develop` and request review — don't merge your own changes without one.
6. Changes to `docs/` (BRD, TRD, DDD, Traceability, Sources) that alter a requirement, gate, or acceptance criterion should note which document(s) they affect in the PR description, since the docs cross-reference each other by ID.

Questions about contributing should go to the repository owner (see SUPPORT.md).
