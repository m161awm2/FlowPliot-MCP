# syntax=docker/dockerfile:1.7
FROM --platform=linux/arm64 python:3.12-slim AS build
COPY --from=ghcr.io/astral-sh/uv:0.13.5 /uv /usr/local/bin/uv
WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy UV_PYTHON_DOWNLOADS=never
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-dev --no-install-project
COPY src ./src
RUN uv sync --frozen --no-dev

FROM --platform=linux/arm64 python:3.12-slim
RUN apt-get update && apt-get install -y --no-install-recommends git ca-certificates \
    && rm -rf /var/lib/apt/lists/* && useradd -r -u 10001 pmcp
COPY --from=build /app /app
ENV PATH=/app/.venv/bin:$PATH PYTHONUNBUFFERED=1
USER pmcp
WORKDIR /app
EXPOSE 8000
CMD ["pmcp", "serve", "--transport", "http", "--port", "8000"]
