from dataclasses import dataclass
import os

from dotenv import load_dotenv


@dataclass(slots=True)
class Settings:
    title: str
    host: str
    port: int
    dark_mode: bool
    storage_secret: str


def load_settings() -> Settings:
    load_dotenv()
    return Settings(
        title=os.getenv("APP_TITLE", "{{PROJECT_TITLE}}"),
        host=os.getenv("APP_HOST", "0.0.0.0"),
        port=int(os.getenv("APP_PORT", "{{APP_PORT}}")),
        dark_mode=os.getenv("DARK_MODE", "true").lower() in ("1", "true", "yes", "on"),
        # Signs the per-user storage cookie. MUST be a real random value in
        # production — set STORAGE_SECRET in your .env / deployment env, e.g.
        #   STORAGE_SECRET=$(openssl rand -hex 32)
        storage_secret=os.getenv("STORAGE_SECRET", "change-me-in-production"),
    )
