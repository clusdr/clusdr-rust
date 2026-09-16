# Contributing

This is the Rust SDK. The daemon lives in [`clusdr`](https://github.com/clusdr/clusdr). Product docs: [Rust SDK](https://clusdr.io/docs/sdk/rust).

License: [Apache-2.0](LICENSE). Conduct: [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md). Security: [SECURITY.md](SECURITY.md).

## Commits

Every commit and pull-request title uses [Conventional Commits](https://www.conventionalcommits.org/):

```text
<type>(optional-scope): <imperative summary>
```

Types we use: `feat`, `fix`, `docs`, `style`, `refactor`, `perf`, `test`, `build`, `ci`, `chore`, `revert`.

Examples:

```text
feat: accept a custom Runtime address in local()
fix: surface Unavailable on a dead socket
docs: point install at the current daemon tag
```

A breaking change uses `feat!:` (or another type with `!`) and a `BREAKING CHANGE:` footer. Subject is lowercase after the type, no trailing period.

CI lints PR commits. Prefer squash-merge; the squash title must stay conventional.

## Requirements

Rust 1.82+.

```bash
make proto    # copy .proto from ../clusdr/proto
cargo test
cargo clippy --all-targets -- -D warnings
```

Generated stubs are produced by `tonic-build` in `build.rs`. Do not check them in; regenerate by copying proto from the daemon repo.
