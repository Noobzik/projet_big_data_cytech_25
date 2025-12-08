FROM ghcr.io/astral-sh/uv:python3.11-bookworm-slim

WORKDIR /app

# Enable bytecode compilation
ENV UV_COMPILE_BYTECODE=1

# Copy project definition
COPY pyproject.toml .

# 1. Create virtual environment
RUN uv venv /opt/venv

# 2. Activate virtual environment for subsequent commands
ENV VIRTUAL_ENV=/opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# 3. Install dependencies into the virtual environment
RUN uv pip install -r pyproject.toml

# Copy source code
COPY . .

EXPOSE 8000

# Script to optionally train model if missing, then start server
CMD ["sh", "-c", "python train_model.py && uvicorn main:app --host 0.0.0.0 --port 8000"]
