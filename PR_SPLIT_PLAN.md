# Plan: Split `feat-index-stats` into Smaller PRs

## Context

The `feat-index-stats` branch has 101 files changed (~11k added, ~2k removed) covering CI, a new pgx scanner, 38+ query files, health gate, collector config, agent interface refactoring, adapter updates, and integration tests. We need to break this into reviewable, sequential PRs where each compiles and passes tests independently.

## Approach

Since commits are interleaved across concerns, we'll create each PR branch from main and cherry-pick or selectively apply the relevant file changes (not cherry-pick whole commits). PRs are ordered so foundational/structural changes land first.

## Dependency Graph

```
PR1 (CI) ──────────────────────────────────────> independent
PR2 (scanner) ──> PR3 (queries + catalog integration tests) ──┐
                                                               ├──> PR5 (agent+adapters+runner) ──> PR6 (remaining tests)
PR4 (healthgate+config+urls) ─────────────────────────────────┘
```

PRs 1, 2, and 4 can be opened in parallel.

---

## PR Sequence

### PR 1: CI & Pre-commit Infrastructure

**Branch:** `pr/ci-precommit` from `main`

**Files:**
- `.github/workflows/check.yml` (add integration-tests job)
- `.pre-commit-config.yaml` (NEW - 5 hooks)
- `Taskfile.yml` (add `build` + `test-integration` tasks)

**Why first:** Zero code deps, all subsequent PRs benefit from CI checks immediately.

**Execution:**
```bash
git checkout -b pr/ci-precommit main
git diff main..feat-index-stats -- .github/workflows/check.yml .pre-commit-config.yaml Taskfile.yml | git apply
go build ./...
# Verify pre-commit hooks run: pre-commit run --all-files
```

---

### PR 2: pgxutil Scanner Package

**Branch:** `pr/pgxutil-scanner` from `main`

**Files:**
- `pkg/internal/pgxutil/scanner.go`
- `pkg/internal/pgxutil/scanner_test.go`

**Why:** Standalone `internal` package, nothing imports it yet. Foundation for all query files. Uses only `pgx/v5` (already a dep).

**Execution:**
```bash
git checkout -b pr/pgxutil-scanner main
git diff main..feat-index-stats -- pkg/internal/pgxutil/ | git apply
go build ./...
go test ./pkg/internal/pgxutil/...
```

---

### PR 3: PG Queries Package + Integration Tests

**Branch:** `pr/pg-queries` from `pr/pgxutil-scanner`

**Files:**
- `pkg/pg/queries/` — all ~38 files including:
  - Core: `collect.go`, `pgtypes.go`, `sanitize.go`, `select1.go`
  - All collectors: `pg_stat_statements.go`, `pg_settings.go`, `pg_class.go`, `connections.go`, `wait_events.go`, etc.
  - All tests: `collect_test.go`, `pg_stat_statements_test.go`, `pg_stats_test.go`, `ddl_test.go`, etc.
- `pkg/pg/pgcatalog_integration_test.go` (tests collectors against PG 13-18 via testcontainers)
- `pkg/pg/testdata/seed.sql`
- `go.mod`/`go.sum` additions for `testcontainers-go`

**Why:** Introduces `CatalogCollector` type, `CollectView` generic, PG type wrappers, and all query implementations. The package compiles but isn't wired in yet. Including integration tests here means each query is validated against real Postgres (13-18) as it lands.

**Depends on:** PR 2 (scanner)

**Execution:**
```bash
git checkout -b pr/pg-queries pr/pgxutil-scanner
git diff main..feat-index-stats -- pkg/pg/queries/ pkg/pg/pgcatalog_integration_test.go pkg/pg/testdata/ | git apply
# Also apply go.mod/go.sum changes for testcontainers-go (manual merge likely needed)
go build ./...
go test ./pkg/pg/queries/...
# Integration: go test -tags integration ./pkg/pg/ -v -timeout 10m
```

---

### PR 4: Health Gate + Collector Config + URL Changes

**Branch:** `pr/healthgate-config-urls` from `main`

**Files:**
- `pkg/pg/healthgate.go`, `pkg/pg/healthgate_test.go`
- `pkg/pg/collector_config.go`, `pkg/pg/collector_config_load.go`, `pkg/pg/collector_config_test.go`
- `pkg/pg/util.go`, `pkg/pg/util_test.go`
- `pkg/dbtune/urls.go`, `pkg/dbtune/urls_test.go`
- `dbtune.yaml`

**Why:** Infrastructure pieces needed by the integration PR. Health gate prevents cascading timeouts. Collector config enables per-collector settings. URL refactoring (`AgentURL`) is prerequisite for `SendCatalogPayload`. All independent of each other but too small for separate PRs.

**Can be opened in parallel with:** PRs 2 and 3 (no mutual deps)

**Execution:**
```bash
git checkout -b pr/healthgate-config-urls main
git diff main..feat-index-stats -- \
  pkg/pg/healthgate.go pkg/pg/healthgate_test.go \
  pkg/pg/collector_config.go pkg/pg/collector_config_load.go pkg/pg/collector_config_test.go \
  pkg/pg/util.go pkg/pg/util_test.go \
  pkg/dbtune/urls.go pkg/dbtune/urls_test.go \
  dbtune.yaml | git apply
go build ./...
go test ./pkg/pg/... ./pkg/dbtune/...
```

---

### PR 5: Agent Interface + CatalogGetter + Adapters + Runner

**Branch:** `pr/agent-adapters-runner` from merge of `pr/pg-queries` + `pr/healthgate-config-urls`

**Files:**
- `pkg/agent/agent.go` (interface: add `ctx` params + `CatalogCollectors`/`SendCatalogPayload`)
- `pkg/agent/catalog_getter.go`, `pkg/agent/catalog_getter_test.go`
- `pkg/agent/collectors.go`, `pkg/agent/ddl.go`
- All 8 adapters: `pkg/{aiven,azureflex,cloudsql,cnpg,docker,patroni,pgprem,rds}/*.go`
- `pkg/runner/runner.go`, `pkg/runner/runner_test.go`
- `pkg/checks/startup.go`
- `cmd/agent.go`
- DELETE `pkg/pg/collectors.go`, DELETE `pkg/pg/queries.go`
- `go.mod`, `go.sum`

**Why:** The interface change forces all adapters + runner to update simultaneously — can't be split further. Old files deleted here when imports switch to new packages.

**Depends on:** PRs 3 + 4

**Execution:**
```bash
# Base off PR 3 and merge PR 4 in
git checkout -b pr/agent-adapters-runner pr/pg-queries
git merge pr/healthgate-config-urls --no-edit
# Apply remaining diffs for agent, adapters, runner, cmd, checks, deletions
git diff main..feat-index-stats -- \
  pkg/agent/ pkg/aiven/ pkg/azureflex/ pkg/cloudsql/ pkg/cnpg/ \
  pkg/docker/ pkg/patroni/ pkg/pgprem/ pkg/rds/ \
  pkg/runner/ pkg/checks/startup.go cmd/agent.go | git apply
# Delete old files
git rm pkg/pg/collectors.go pkg/pg/queries.go
go build ./...
go test ./...
```

---

### PR 6: Remaining Tests + Test Infrastructure

**Branch:** `pr/remaining-tests` from `pr/agent-adapters-runner`

**Files:**
- `pkg/pg/healthgate_integration_test.go`
- `pkg/dbtune/dbtunetest/transport.go`
- `pkg/agent/agent_test.go` (updated tests)

**Why:** Health gate integration tests need health gate (PR 4) + full pipeline (PR 5). `dbtunetest/transport.go` mocks the dbtune API for agent/runner tests. Smaller PR now that catalog integration tests moved to PR 3.

**Depends on:** PR 5

**Execution:**
```bash
git checkout -b pr/remaining-tests pr/agent-adapters-runner
git diff main..feat-index-stats -- \
  pkg/pg/healthgate_integration_test.go \
  pkg/dbtune/dbtunetest/ \
  pkg/agent/agent_test.go | git apply
go build ./...
go test ./...
# Integration: go test -tags integration ./pkg/pg/ -v -timeout 10m
```

---

## Verification Checklist

For **every** PR:
- [ ] `go build ./...` passes
- [ ] `go test ./...` passes

Additional:
- [ ] PR 1: `pre-commit run --all-files` works
- [ ] PR 3: `go test -tags integration ./pkg/pg/ -v -timeout 10m` (requires Docker)
- [ ] PR 6: `go test -tags integration ./pkg/pg/ -v -timeout 10m` (health gate integration)

## Notes

- `go.mod`/`go.sum` changes will need manual merging in PRs 3 and 5 since `git diff | git apply` on these files often conflicts.
- If `git apply` fails for a file, fall back to manually copying the file content from `feat-index-stats`: `git show feat-index-stats:<path> > <path>`.
- Each PR description should reference which PR(s) it depends on and link them.
- After each PR merges, rebase downstream branches onto `main`.
