# ==============================================================================
# CodeClaim Production Containerfile
# Packages the unified FastAPI Coordinator, Dashboard UI, Background Workers,
# and CLI tools into a single deployment artifact.
# ==============================================================================

FROM python:3.12-slim

WORKDIR /app

# Install system dependencies & Git for worktree isolation
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy project definition and install dependencies
COPY pyproject.toml .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -e .

# Copy application source code
COPY coordinator/ coordinator/
COPY repos/ repos/
COPY infra/ infra/
COPY scripts/ scripts/

# Expose internal coordinator port (bound to loopback / reverse proxy)
EXPOSE 8000

ENV PYTHONUNBUFFERED=1
ENV HOST=0.0.0.0
ENV PORT=8000

# Start unified coordinator (serves dashboard UI, APIs, and background workers)
CMD ["uvicorn", "coordinator.app:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
