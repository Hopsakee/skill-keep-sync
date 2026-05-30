FROM ghcr.io/astral-sh/uv:python3.12-trixie-slim
WORKDIR /app
COPY pyproject.toml uv.lock .
RUN --mount=type=cache,target=/root/.cache uv sync --no-install-project
COPY . .
RUN --mount=type=cache,target=/root/.cache uv sync
EXPOSE {{APP_PORT}}
CMD ["uv", "run", "main.py"]
