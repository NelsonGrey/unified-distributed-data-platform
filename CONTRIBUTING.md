# Contributing

Unified Distributed Data Platform is closed-source. The source is publicly visible, but this isn't an open-source project — there's no public issue tracker and outside pull requests aren't accepted.

If you have collaborator access to this repository:

1. Branch from `develop` (`feature/<short-description>` or `fix/<short-description>`) — `develop` is the active integration branch; `staging` and `main` are only updated via promotion PRs.
2. Keep commits focused, and write commit messages that explain *why*, not just *what*.
3. Before opening a pull request, run `go build ./...`, `go vet ./...`, and `go test ./...`, and make sure `gofmt -l .` reports no files.
4. Open the PR against `develop` and request review — don't merge your own changes without one.
5. Changes to `docs/` (BRD, TRD, DDD, Traceability, Sources) that alter a requirement, gate, or acceptance criterion should note which document(s) they affect in the PR description, since the docs cross-reference each other by ID.

Questions about contributing should go to the repository owner (see SUPPORT.md).
