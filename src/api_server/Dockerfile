FROM python:3.12-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

COPY pyproject.toml uv.lock* ./
RUN uv sync --frozen --no-install-project 2>/dev/null || uv sync --no-install-project

COPY . .
RUN uv sync --frozen 2>/dev/null || uv sync

CMD ["uv", "run", "uvicorn", "src.server.main:app", "--host", "0.0.0.0", "--port", "8000"]
