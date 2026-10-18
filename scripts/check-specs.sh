#!/usr/bin/env bash
# check-specs.sh - model-check the TLA+ specs in spec/ and compare each verdict
# with spec/expected.txt.
#
# A config listed as "pass" must pass. A config listed with an invariant must
# violate exactly that invariant: those are the known counterexamples, kept as
# configs so a fix has to flip them to "pass" on purpose, and so a spec change
# that quietly stops finding them fails here instead of looking like a fix.
#
# Needs Java 11+. TLC comes from the tlaplus v1.7.4 release, checked against its
# sha256 and cached under ~/.cache/gate. Exits 1 on any unexpected verdict.
#
# SPEC=Quota limits the run to one module; TRACE=1 also prints the trace of
# every expected violation.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
SPEC_DIR="$ROOT/spec"

TLA_VERSION=v1.7.4
TLA_SHA256=936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88
CACHE="${XDG_CACHE_HOME:-$HOME/.cache}/gate"
JAR="$CACHE/tla2tools-$TLA_VERSION.jar"

command -v java >/dev/null 2>&1 || { echo "Error: java not found in PATH." >&2; exit 1; }

if [ ! -f "$JAR" ]; then
  mkdir -p "$CACHE"
  curl -sSLf -o "$JAR.part" \
    "https://github.com/tlaplus/tlaplus/releases/download/$TLA_VERSION/tla2tools.jar"
  mv "$JAR.part" "$JAR"
fi
echo "$TLA_SHA256  $JAR" | sha256sum -c --quiet - || {
  echo "Error: $JAR does not match its pinned sha256; delete it and rerun." >&2
  exit 1
}

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

failures=0
while read -r module cfg expected; do
  case "$module" in ''|'#'*) continue ;; esac
  [ -n "${SPEC:-}" ] && [ "$SPEC" != "$module" ] && continue

  out="$WORK/$cfg.out"
  start=$(date +%s)
  # TLC exits non-zero on a violation, which is often the expected verdict.
  (cd "$SPEC_DIR" && java -XX:+UseParallelGC -cp "$JAR" tlc2.TLC \
     -workers auto -metadir "$WORK/states-$cfg" -config "$cfg" "$module.tla") \
     >"$out" 2>&1 || true
  secs=$(( $(date +%s) - start ))
  states=$(grep -oE '[0-9]+ distinct states found' "$out" | tail -1 | cut -d' ' -f1)

  if grep -q 'Model checking completed. No error has been found.' "$out"; then
    got=pass
  elif violated=$(grep -oE 'Invariant [A-Za-z0-9_]+ is violated' "$out" | head -1); then
    got=$(echo "$violated" | cut -d' ' -f2)
  else
    got=error
  fi

  if [ "$got" = "$expected" ]; then
    printf 'ok    %-26s %-20s %10s states  %4ss\n' "$cfg" "$got" "${states:-?}" "$secs"
    if [ -n "${TRACE:-}" ] && [ "$got" != pass ]; then
      sed -n '/^Error:/,/^[0-9]* states generated/p' "$out"
    fi
  else
    printf 'FAIL  %-26s expected %s, got %s\n' "$cfg" "$expected" "$got"
    sed -n '/^Error:/,$p' "$out" | head -200
    [ "$got" = error ] && tail -30 "$out"
    failures=$((failures + 1))
  fi
done < "$SPEC_DIR/expected.txt"

[ "$failures" -eq 0 ] || { echo "$failures spec verdict(s) unexpected." >&2; exit 1; }
