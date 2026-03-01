FROM python:3.12.9-slim

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
CMD ["uv", "run", "/workspace/main.py", "--transaction_rules", "/rules/transaction_rules.yaml", "--prompt_file", "/rules/prompt.txt"]