#!/usr/bin/env bash
#
# Publish every workspace crate that is not already on crates.io.
#
# `cargo publish --workspace` cannot do this. It aborts the moment it meets a
# crate whose version is already published, which makes it useless for the two
# things that actually happen during a first release:
#
#   * crates.io rate limits *new* crate names to roughly one per ten minutes, so
#     a workspace of any size is guaranteed to be interrupted partway;
#   * an interrupted release then cannot be resumed, because the crates that did
#     go out now trip the abort.
#
# Note also that `cargo publish --dry-run` does not consult the registry for
# existing versions, so it cannot be used to predict either failure.
#
# This script is idempotent: it asks crates.io what is already there, publishes
# only the remainder in dependency order, and waits out each rate limit. Running
# it twice is harmless.
#
#   DRY_RUN=1  print the plan and exit without publishing.
set -uo pipefail

DRY_RUN="${DRY_RUN:-0}"
UA='vituss-release (https://github.com/dixonSolutions/Vituss)'

# Topologically sort the workspace: a crate is only publishable once every
# workspace sibling it depends on is on the registry.
order_and_versions() {
  cargo metadata --no-deps --format-version 1 | python3 -c '
import json, sys
md = json.load(sys.stdin)
pkgs = {p["name"]: p for p in md["packages"] if p.get("publish") != []}
names = set(pkgs)
deps = {n: {d["name"] for d in p["dependencies"] if d["name"] in names} for n, p in pkgs.items()}
done, out = set(), []
while len(done) < len(names):
    ready = sorted(n for n in names if n not in done and deps[n] <= done)
    if not ready:
        sys.exit("cycle among workspace crates: " + ", ".join(sorted(names - done)))
    for n in ready:
        out.append((n, pkgs[n]["version"]))
        done.add(n)
print("\n".join(f"{n} {v}" for n, v in out))
'
}

already_published() { # name version
  curl -sS --retry 3 -H "User-Agent: $UA" "https://crates.io/api/v1/crates/$1/versions" 2>/dev/null \
    | grep -q "\"num\":\"$2\""
}

plan=$(order_and_versions) || exit 1
echo "$plan" | while read -r name version; do echo "  $name $version"; done

published=0 skipped=0
while read -r name version; do
  [ -n "$name" ] || continue

  if already_published "$name" "$version"; then
    echo "skip    $name $version (already on crates.io)"
    skipped=$((skipped + 1))
    continue
  fi

  if [ "$DRY_RUN" = "1" ]; then
    echo "would publish $name $version"
    continue
  fi

  for attempt in $(seq 1 12); do
    echo "publish $name $version (attempt $attempt)"
    out=$(cargo publish -p "$name" --locked 2>&1); rc=$?
    if [ $rc -eq 0 ]; then
      echo "ok      $name $version"
      published=$((published + 1))
      break
    fi
    # Someone else got there first between the check and the upload.
    if grep -q "already exists on crates.io" <<<"$out"; then
      echo "skip    $name $version (published concurrently)"
      break
    fi
    when=$(grep -oP 'try again after \K.*?(?= and see)' <<<"$out" | head -1)
    if [ -z "$when" ]; then
      echo "FAILED  $name $version"
      grep -E 'error|Caused by' <<<"$out" | head -5
      exit 1
    fi
    wait=$(( $(date -u -d "$when" +%s) - $(date -u +%s) + 20 ))
    [ "$wait" -lt 20 ] && wait=20
    echo "        rate limited, waiting $(( wait / 60 ))m$(( wait % 60 ))s"
    sleep "$wait"
  done
done <<<"$plan"

# Nothing may be left behind: re-ask the registry rather than trusting the loop.
missing=""
while read -r name version; do
  [ -n "$name" ] || continue
  already_published "$name" "$version" || missing="$missing $name@$version"
done <<<"$plan"

if [ "$DRY_RUN" = "1" ]; then
  [ -n "$missing" ] && echo "dry run: would publish$missing" || echo "dry run: nothing to do"
  exit 0
fi
if [ -n "$missing" ]; then
  echo "still not published:$missing"
  exit 1
fi
echo "all workspace crates are on crates.io"
