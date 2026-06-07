"""Shared layout: header, navigation, page wrapper."""

from __future__ import annotations

from contextlib import contextmanager
from typing import Generator

from nicegui import app, ui

APP_CSS = """
:root, body.body--dark {
    --bg-primary: #0f172a;
    --bg-secondary: #1e293b;
    --bg-card: #1e293b;
    --text-primary: #f1f5f9;
    --text-secondary: #94a3b8;
    --text-muted: #64748b;
    --accent: #38bdf8;
    --accent-hover: #7dd3fc;
    --border: #334155;
    --hover-bg: rgba(148, 163, 184, 0.08);
    --header-bg: #0f172a;
}

body.body--light {
    --bg-primary: #f8fafc;
    --bg-secondary: #ffffff;
    --bg-card: #ffffff;
    --text-primary: #0f172a;
    --text-secondary: #475569;
    --text-muted: #94a3b8;
    --accent: #0284c7;
    --accent-hover: #0369a1;
    --border: #e2e8f0;
    --hover-bg: rgba(15, 23, 42, 0.04);
    --header-bg: #ffffff;
}

body {
    background-color: var(--bg-primary) !important;
    color: var(--text-primary) !important;
    font-family: 'Inter', 'system-ui', sans-serif;
}

.q-page { background-color: var(--bg-primary) !important; }
.q-layout { background-color: var(--bg-primary) !important; }

.app-card {
    background-color: var(--bg-card);
    border: 1px solid var(--border);
    border-radius: 12px;
    padding: 1.25rem;
    transition: border-color 0.2s, transform 0.15s;
}
.app-card:hover {
    border-color: var(--accent);
    transform: translateY(-2px);
}

/* Header */
.app-header {
    background-color: var(--header-bg) !important;
    border-bottom: 1px solid var(--border) !important;
}
"""


def add_head_html() -> None:
    ui.add_head_html(f"<style>{APP_CSS}</style>")
    ui.add_head_html(
        '<link rel="preconnect" href="https://fonts.googleapis.com">'
        '<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>'
        '<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">'
    )


def header() -> None:
    with ui.header().classes("app-header"):
        with ui.row().classes("w-full items-center px-6 py-2"):
            ui.link("{{PROJECT_TITLE}}", "/").classes(
                "text-xl font-bold no-underline"
            ).style("color: var(--accent)")
            ui.space()
            ui.link("Home", "/").classes("no-underline text-sm").style(
                "color: var(--text-secondary)"
            )

            is_dark = app.storage.user.get("dark_mode", True)
            dark = ui.dark_mode(value=is_dark)

            def toggle_dark() -> None:
                dark.toggle()
                app.storage.user["dark_mode"] = dark.value

            icon = "light_mode" if is_dark else "dark_mode"
            ui.button(
                icon=icon, on_click=toggle_dark
            ).props("flat round size=sm").style("color: var(--text-secondary)")


@contextmanager
def page_layout(title: str = "") -> Generator[None, None, None]:
    add_head_html()
    if title:
        ui.page_title(f"{title} â {{PROJECT_TITLE}}")
    header()
    with ui.column().classes("w-full max-w-6xl mx-auto px-6 py-8 gap-6"):
        yield
