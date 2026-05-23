# Contributing

Thanks for improving kq. This project is intended to stay small, public, and
easy to verify.

## Ground Rules

- Keep changes focused on Kubernetes snapshot analysis.
- Use synthetic data in tests and docs. Generate fixtures with
  `//kq/src/bin:synthetic_snapshot` or hand-write minimal synthetic Kubernetes
  objects.
- Do not commit real cluster snapshots, private hostnames, registry names,
  internal domains, user data, secrets, tokens, or company-specific labels.
- Prefer clear, boring interfaces over one-off scripts.
- Add tests for behavior changes and document new user-facing workflows.

## Development Workflow

```bash
bazel build -c opt //kq/src:kq
bazel test //kq/...
```

For loader, schema, query-registration, or output changes, also run the focused
suite documented in [CLAUDE.md](CLAUDE.md#when-changing-loader--schema--query-registration--output)
and the end-to-end validation flow in
[docs/DEVELOPMENT.md](docs/DEVELOPMENT.md#validation-workflow).

## Pull Request Checklist

- Explain the user-facing change or developer-facing cleanup.
- Include the Bazel commands you ran.
- Update `README.md` or `docs/` when behavior changes.
- Keep helper binaries documented and avoid hard-coded local paths.
- Confirm new fixtures are synthetic and safe to publish.

## Adding Dependencies

See [docs/DEVELOPMENT.md#dependency-updates](docs/DEVELOPMENT.md#dependency-updates).
