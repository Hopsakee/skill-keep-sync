"""Home page."""

from nicegui import ui

from app.components.layout import page_layout
from app.config import Settings


def register(settings: Settings) -> None:

    @ui.page("/")
    def home_page() -> None:
        with page_layout("Home"):
            ui.label("{{PROJECT_TITLE}}").style(
                "color: var(--text-primary); font-size: 1.875rem; font-weight: 700"
            )
            ui.label("Your NiceGUI app is running.").style(
                "color: var(--text-muted); font-size: 0.875rem"
            )

            with ui.element("div").classes("app-card"):
                ui.label("Getting started").style(
                    "color: var(--text-primary); font-weight: 600"
                )
                ui.label(
                    "Edit app/pages/home.py to customize this page. "
                    "Add new pages in app/pages/ and register them in main.py."
                ).style("color: var(--text-secondary); font-size: 0.875rem")
