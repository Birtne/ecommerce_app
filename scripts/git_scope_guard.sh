#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "${repo_root}"

allow_regex='^(ecommerce_app/|\.github/workflows/ecommerce-ci\.yml$|\.github/workflows/nightly-monitoring-drill\.yml$|\.gitignore$)'

check_list() {
  local title="$1"
  local data="$2"
  if [[ -z "${data}" ]]; then
    return 0
  fi
  local bad
  bad="$(printf '%s\n' "${data}" | sed '/^$/d' | grep -Ev "${allow_regex}" || true)"
  if [[ -n "${bad}" ]]; then
    echo "[scope-guard] ${title} contains non-delivery paths:"
    printf '%s\n' "${bad}"
    exit 1
  fi
}

staged="$(git diff --cached --name-only)"
unstaged="$(git diff --name-only)"
untracked="$(git ls-files --others --exclude-standard)"

check_list "staged changes" "${staged}"
check_list "unstaged changes" "${unstaged}"
check_list "untracked files" "${untracked}"

echo "[scope-guard] ok"
