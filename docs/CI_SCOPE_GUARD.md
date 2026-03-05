# CI Scope Guard Policy (iter-13)

## Goal
- Keep release branch traceable.
- Prevent accidental commits from unrelated workspace paths.

## Mandatory Check
- CI job: `scope-guard` in `.github/workflows/ecommerce-ci.yml`
- Script: `scripts/git_scope_guard.sh`
- Script: `scripts/frontend_drift_guard.sh`
- The check fails if staged/unstaged/untracked paths are outside allowed scope.
- The frontend guard blocks protected entrypoint drift by default:
  - `ecommerce_app/frontend/src/App.tsx`
  - `ecommerce_app/frontend/src/styles.css`
- On pull requests, CI posts/updates a drift comment with blocked file list and isolation suggestion.
- CI also generates executable isolation script artifact path (default: `/tmp/ecom-ci/frontend-drift-isolate.sh`) for direct copy/paste execution in pipeline logs.
- Drift report now includes target-scope commit template commands (`target_commit_commands`) to stage only non-protected frontend files after isolation.

## Default Allowed Paths
- `ecommerce_app/**`
- `.github/workflows/ecommerce-ci.yml`
- `.github/workflows/nightly-monitoring-drill.yml`
- `.gitignore`

## Whitelist Exception Process
1. Create a PR that includes:
   - why the non-default path is needed,
   - risk impact,
   - rollback plan.
2. Update allowlist regex in `scripts/git_scope_guard.sh`.
3. Add/update this document with:
   - exact path added,
   - owner,
   - expiration date for exception.
4. Require approval from backend + infra/code-owner reviewers.
5. Merge only after `scope-guard` and full CI pass.

## Current Exceptions
- None.

## Frontend Drift Exception
1. Set `FRONTEND_DRIFT_ALLOW_REGEX` in CI/job env for the exact protected file path.
2. Include root-cause and rollback notes in PR description.
3. Remove the override in the same PR or next cleanup PR.

## Local Noise Isolation Strategy
- Ignore temporary local artifacts via `.gitignore`:
  - `.pw-browsers/`
  - `pwshot/`
  - `ecommerce_app/package.json`
  - `ecommerce_app/package-lock.json`
- If root-level npm manifest is needed in future, use a dedicated PR with explicit scope and update this document.
