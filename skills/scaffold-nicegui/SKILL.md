---
name: ScaffoldNicegui
model: sonnet
description: Scaffold a new NiceGUI webapp project with working dark/light mode, CSS variable theming, Docker deployment, and agent instructions. USE WHEN scaffold nicegui, new nicegui app, create nicegui project, start nicegui project, bootstrap nicegui.
---

## Customization

**Before executing, check for user customizations at:**
`~/.claude/PAI/USER/SKILLCUSTOMIZATIONS/ScaffoldNicegui/`

If this directory exists, load and apply any PREFERENCES.md, configurations, or resources found there. These override default behavior. If the directory does not exist, proceed with skill defaults.

## 🚨 MANDATORY: Voice Notification (REQUIRED BEFORE ANY ACTION)

**You MUST send this notification BEFORE doing anything else when this skill is invoked.**

1. **Send voice notification**: Voice notify per `skills/CLAUDE.md` § Voice notification, message: "Running the Scaffold workflow in the ScaffoldNiceGUI skill to create a new NiceGUI project"

2. **Output text notification**:
   ```
   Running the **Scaffold** workflow in the **ScaffoldNicegui** skill to create a new NiceGUI project...
   ```

**This is not optional. Execute this curl command immediately upon skill invocation.**

# ScaffoldNiceGUI

Scaffold a complete, working NiceGUI webapp project with all conventions from the pkw-web experience baked in.

## What it creates

A project directory with:
- `main.py` — entry point with `ui.run()`, `storage_secret`, dark mode
- `app/config.py` — dataclass Settings from env vars
- `app/components/layout.py` — shared layout with CSS variable theming, dark/light toggle, header
- `app/pages/home.py` — sample home page using `page_layout` context manager
- `pyproject.toml` — uv dependencies (nicegui, httpx, python-dotenv)
- `.python-version` — pins 3.12
- `Dockerfile` — uv:python3.12-trixie-slim pattern
- `compose.yaml` — hopsakee-server hup network pattern
- `.gitignore`, `.dockerignore`
- `AGENTS.md`, `AGENTS-main.md`, `skills/` — copied from agent-instruct-nicegui

## Execution

### Step 1: Gather parameters

Ask the user (via AskUserQuestion) for:

| Parameter | Placeholder | Default | Example |
|-----------|-------------|---------|---------|
| Project name | `{{PROJECT_NAME}}` | (required) | `my-app` |
| Project title | `{{PROJECT_TITLE}}` | Titlecased project name | `My App` |
| Project description | `{{PROJECT_DESCRIPTION}}` | `A NiceGUI web application` | `Dashboard for sensor data` |
| Port | `{{APP_PORT}}` | `8080` | `8080` |
| Target directory | — | `~/Code/{{PROJECT_NAME}}` | `~/Code/my-app` |
| Additional deps | — | none | `python-frontmatter markdown` |
| Create GitHub repo? | — | yes | yes/no |

### Step 2: Copy template files

1. Read all files from `~/.claude/skills/ScaffoldNicegui/template/`
2. For each file, replace all `{{PLACEHOLDER}}` values with the gathered parameters
3. Write each file to the target directory, preserving the directory structure

### Step 3: Copy agent instructions

Copy from `~/Code/agent-instruct-nicegui/`:
- `AGENTS.md` → project root
- `AGENTS-main.md` → project root
- `skills/` directory → project root

If the repo doesn't exist locally, clone it:
```bash
gh repo clone Hopsakee/agent-instruct-nicegui ~/Code/agent-instruct-nicegui
```

### Step 4: Install dependencies

```bash
cd {target_dir}
uv sync
```

If additional deps were specified:
```bash
uv add {additional_deps}
```

### Step 5: Initialize git

```bash
cd {target_dir}
git init
git branch -m main
git add -A
git commit -m "Initial scaffold from ScaffoldNiceGUI skill"
```

(Let git use the user's existing global `user.name` / `user.email` — do not hardcode an identity.)

### Step 6: Create GitHub repo (if requested)

```bash
gh repo create Hopsakee/{PROJECT_NAME} --public --description "{PROJECT_DESCRIPTION}" --source . --push
```

### Step 7: Verify

1. Run `uv run main.py` briefly to confirm the app starts
2. Confirm the home page returns HTTP 200
3. **Use the Browser skill to take a screenshot and verify the UI renders correctly**
4. Kill the test server
5. Report the project location and GitHub URL

## Post-scaffold guidance

After scaffolding, tell the user:
- "Your NiceGUI app is ready at {target_dir}"
- "Run `uv run main.py` to start (port {APP_PORT})"
- "AGENTS.md is in place — AI agents will follow NiceGUI conventions"
- "Dark/light toggle, CSS variable theming, and Docker deployment are pre-configured"
