#!/usr/bin/env bash
set -eu

# Run from repo root
cd "$(pwd)"

if [ -f creditcardfraud.csv ]; then
  cp creditcardfraud.csv /tmp/creditcardfraud.csv.backup
  echo "BACKUP_CREATED:/tmp/creditcardfraud.csv.backup"
else
  echo "NO_LOCAL_FILE"
fi

echo "-- Removing file from index (if present) --"
git rm --cached --ignore-unmatch creditcardfraud.csv || true

if git rev-parse --verify HEAD >/dev/null 2>&1; then
  echo "-- Rewriting history to remove creditcardfraud.csv --"
  git filter-branch --force --index-filter 'git rm --cached --ignore-unmatch creditcardfraud.csv' --prune-empty -- --all
else
  echo "No commits to rewrite"
fi

# Remove refs created by filter-branch
if git for-each-ref --format='%(refname)' refs/original/ | grep -q .; then
  git for-each-ref --format='%(refname)' refs/original/ | xargs -r git update-ref -d || true
fi

echo "-- Expire reflog and run gc --"
git reflog expire --expire=now --all || true
git gc --prune=now --aggressive || true

echo "-- Force pushing cleaned history to origin --"
git push origin --force --all || true

# Ensure .gitignore contains the file
if ! grep -qx "creditcardfraud.csv" .gitignore 2>/dev/null; then
  echo "creditcardfraud.csv" >> .gitignore
  git add .gitignore
  git commit -m "Ignore large dataset creditcardfraud.csv" || true
  git push origin HEAD:main --force || true
else
  echo ".gitignore already contains creditcardfraud.csv"
fi

echo "DONE"
