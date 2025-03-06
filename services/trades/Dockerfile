FROM python:3.10-slim-bookworm
# 4) Copy uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

ADD . /app

WORKDIR /app

RUN uv sync --frozen

CMD ["uv", "run", "run.py"]