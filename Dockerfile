FROM python:3.13-slim

# Install system dependencies and uv
RUN apt-get update && \
    apt-get install -y --no-install-recommends build-essential && \
    rm -rf /var/lib/apt/lists/* && \
    pip install --upgrade pip && pip install uv

# Set working directory
WORKDIR /workspace

# Copy only dependency files first for better caching
COPY pyproject.toml uv.lock ./

# Install Python dependencies
RUN uv pip install -r pyproject.toml --system

# Copy only necessary source and config files
COPY app/ ./app/
COPY main.py ./main.py

# Set entrypoint (use exec form for proper signal handling)
# Use the system interpreter directly: dependencies are already installed
# system-wide above, and `uv run` would otherwise recreate a project venv
# (downloading an interpreter + all deps) on every container start.
ENTRYPOINT ["python", "/workspace/main.py"]
CMD ["--source", "email"]