#!/usr/bin/env sh
set -eu

terms="$(printf '%s\\n' 'od''oo' 'part''ner' 'le''ad' 'c''rm')"
matches="$(git grep -I -n -i -F -f - -- . ':!scripts/check-public-boundary.sh' <<EOF || true
$terms
EOF
)"

if [ -n "$matches" ]; then
  printf '%s\\n' "Public boundary violation:" "$matches" >&2
  exit 1
fi

if git log --all --format=%B | grep -i -E 'od''oo|part''ner|(^|[^a-z])le''ad([^a-z]|$)|(^|[^a-z])c''rm([^a-z]|$)' >/dev/null; then
  echo "Public boundary violation in commit history" >&2
  exit 1
fi

cyrillic_pattern='[\x{0400}-\x{052f}\x{1c80}-\x{1c8f}\x{2de0}-\x{2dff}\x{a640}-\x{a69f}]'
cyrillic_matches="$(git grep -I -n -P "$cyrillic_pattern" -- . || true)"

if [ -n "$cyrillic_matches" ]; then
  printf '%s\n' "Non-English public content:" "$cyrillic_matches" >&2
  exit 1
fi

if git log --all --format=%B | python3 -c '
import sys
import unicodedata

text = sys.stdin.read()
found = any(unicodedata.name(char, "").startswith("CYRILLIC") for char in text)
raise SystemExit(0 if found else 1)
'; then
  echo "Non-English public content in commit history" >&2
  exit 1
fi
