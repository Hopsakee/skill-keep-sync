---
name: ship
description: "Git add, commit, and push in one command. USE WHEN ship, commit and push, git ship, push it, send it, commit push, acp."
title: ship
active_version: 1
---

# Ship

One-command git add + commit + push. Drafts a commit message from the diff, stages files explicitly, commits, and pushes.

## Arguments

Optional: a commit message hint. E.g., `ship fix the login bug` uses that as guidance for the commit message.

## Steps

### 1. Assess changes (parallel)

Run these three commands in parallel:

```bash
git status
```
```bash
git diff HEAD
```
```bash
git log --oneline -5
```

If there are no changes (nothing to commit), tell the user and stop.

### 2. Draft commit message

From the diff and recent log style:

- Summarize the nature of the changes (new feature, enhancement, bug fix, refactor, etc.)
- Write a concise commit message (1-3 sentences) focusing on *why*, not *what*
- Match the commit style from `git log`
- If the user provided a hint in the arguments, use it as the basis

### 3. Stage, commit, and push

```bash
git add <specific files from status>
```

Do NOT use `git add -A` or `git add .` â list files explicitly to avoid accidentally staging secrets or large binaries. Skip `.env`, `credentials.*`, and similar sensitive files.

```bash
git commit -m "<commit message>"
```

```bash
git push
```

If the branch has no upstream, use `git push -u origin <branch>` instead.

### 4. Confirm

Report the commit hash and what was pushed. If the commit or push fails, diagnose and report the error â do not retry silently.
