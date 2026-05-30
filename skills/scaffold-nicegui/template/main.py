from app.config import load_settings
from app.pages import home
from nicegui import ui


def main() -> None:
    settings = load_settings()

    home.register(settings)

    ui.run(
        title=settings.title,
        host=settings.host,
        port=settings.port,
        reload=False,
        show=False,
        dark=settings.dark_mode,
        storage_secret=settings.storage_secret,
    )


if __name__ == "__main__":
    main()
