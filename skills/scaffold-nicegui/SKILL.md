---
name: scaffold-nicegui
description: Scaffold a new NiceGUI webapp project with working dark/light mode, CSS variable theming, Docker deployment, and a clean app/ structure. USE WHEN scaffold nicegui, new nicegui app, create nicegui project, start nicegui project, bootstrap nicegui.
---

# Scaffold NiceGUI

Scaffold a complete, working NiceGUI webapp project with production-ready conventions baked in: dark/light theming via CSS variables, a shared layout context manager, env-driven config, and a Docker setup.

## What it creates

A project directory with:

- `main.py` — entry point with `ui.run()`, env-driven `storage_secret`, dark mode
- `app/config.py` — dataclass `Settings` loaded from env vars
- `app/components/layout.py` — shared layout: CSS variable theming, dark/light toggle, header, `page_layout` context manager
- `app/pages/home.py` — sample home page using the `page_layout` context manager
- `pyproject.toml` — uv dependencies (nicegui, httpx, python-dotenv) + pytest dev group
- `.python-version` — pins 3.12
- `Dockerfile` — `astral-sh/uv` python3.12 pattern
- `compose.yaml` — single-service compose publishing the app port
- `.gitignore`, `.dockerignore`

## Execution

### Step 1: Gather parameters

Ask the user (via AskUserQuestion or directly) for:

| Parameter | Placeholder | Default | Example |
|-----------|-------------|---------|---------|
| Project name | `{{PROJECT_NAME}}` | (required) | `my-app` |
| Project title | `{{PROJECT_TITLE}}` | Titlecased project name | `My App` |
| Project description | `{{PROJECT_DESCRIPTION}}` | `A NiceGUI web application` | `Dashboard for sensor data` |
| Port | `{{APP_PORT}}` | `8080` | `8080` |
| Target directory | — | `./{{PROJECT_NAME}}` | `~/code/my-app` |
| Additional deps | — | none | `python-frontmatter markdown` |
| Create GitHub repo? | — | ask | yes/no |

### Step 2: Copy template files

1. Read all files from this skill's `template/` directory.
2. For each file, replace every `{{PLACEHOLDER}}` with the gathered parameters.
3. Write each file to the target directory, preserving the directory structure.

### Step 3: Install dependencies

```bash
cd {target_dir}
uv sync
```

If additional deps were specified:

```bash
uv add {additional_deps}
```

### Step 4: Initialize git

```bash
cd {target_dir}
git init
git branch -m main
git add -A
git commit -m "Initial scaffold from scaffold-nicegui skill"
```

(Let git use the user's existing global `user.name` / `user.email` — do not hardcode an identity.)

### Step 5: Create GitHub repo (if requested)

```bash
gh repo create {{PROJECT_NAME}} --public --description "{{PROJECT_DESCRIPTION}}" --source . --push
```

Use `--private` instead of `--public` if the user prefers. Prefix with an org (`gh repo create <org>/{{PROJECT_NAME}} ...`) only if the user names one.

### Step 6: Verify

1. Set a real `STORAGE_SECRET` env var (see note below), then run `uv run main.py` briefly to confirm the app starts.
2. Confirm the home page returns HTTP 200 (e.g. `curl -s -o /dev/null -w '%{http_code}' http://localhost:{{APP_PORT}}/`).
3. Open the page in a browser (or screenshot it) to confirm the UI renders and the dark/light toggle works.
4. Kill the test server.
5. Report the project location and (if created) the GitHub URL.

## Security note — storage secret

NiceGUI needs a `storage_secret` to sign the per-user storage cookie. The template reads it from the `STORAGE_SECRET` env var. **Set a real, random value in production** (e.g. `STORAGE_SECRET=$(openssl rand -hex 32)` in your `.env` or deployment environment). The template default is a placeholder and must not be used in production.

## Post-scaffold guidance

After scaffolding, tell the user:

- "Your NiceGUI app is ready at {target_dir}"
- "Set `STORAGE_SECRET` in a `.env` file, then run `uv run main.py` to start (port {{APP_PORT}})"
- "Dark/light toggle, CSS variable theming, and a Docker setup are pre-configured"
- "Add new pages in `app/pages/` and register them in `main.py`"
