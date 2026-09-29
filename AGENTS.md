# Working on this adapter

Read README.md and docs/telemetry.md before changing collection or signal semantics.

- Keep this repository deployment-neutral. Use synthetic records and reserved
  example domains in documentation, fixtures, dashboards and tests.
- Private endpoints, client identities, credentials, source payloads and runtime
  screenshots belong only in ignored local storage or private external storage.
- Do not copy private configuration into issues, commit messages or build contexts.
- Keep the adapter OTLP-only. Backend-specific routing belongs in the sample
  receiver configuration or the operator's observability pipeline.
- Source checkpoint and deduplication changes require crash/replay tests.
- Preserve missing/forbidden/truncated data as coverage gaps, not zeros.
- Run go test -race ./... and go vet ./... for code changes.
- Regenerate dashboard JSON with python3 scripts/generate-dashboard.py.
- Use the synthetic Compose profile for reproducible integration tests. Live
  reads require private local configuration; never commit captured live fixtures.
