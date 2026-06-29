#!/usr/bin/env bash
# Detect functions unreachable from main ("dead code") via x/tools deadcode.
# Fails if any are found, except an explicit allowlist of known false positives.
set -euo pipefail

# Pinned by commit SHA (the trailing comment is the tag it resolves to),
# matching the SHA-pinning convention used for GitHub Actions in this repo.
# The "# v0.37.0" is a shell comment and is not part of the value.
DEADCODE=golang.org/x/tools/cmd/deadcode@d49da96b802368c7325ac3b662a238c09a0615b9 # v0.37.0

# Accepted "unreachable" funcs (deadcode false positives):
#   *.isApplyConfigError - sealed-interface marker methods. Never called, but
#   required to keep the ApplyConfigError set closed to its package
#   (see pkg/agent/agent.go). Extend this regex for new sealed interfaces.
ALLOW_RE='unreachable func: .*\.isApplyConfigError$'

cd "$(dirname "$0")/.."

findings=$(go run "$DEADCODE" -test ./... | { grep -vE "$ALLOW_RE" || true; })

if [ -n "$findings" ]; then
  echo "Dead code detected (functions unreachable from main):"
  echo "$findings"
  echo
  echo "Remove the dead code. If this is a genuine false positive (e.g. a"
  echo "sealed-interface marker method), add it to ALLOW_RE in scripts/deadcode.sh."
  exit 1
fi

echo "deadcode: no unreachable functions."
