# Multi-stage build for PulseVortex Monitor
FROM python:3.14-slim as base

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Set work directory
WORKDIR /app

# Copy requirements and install Python dependencies
COPY requirements.txt pyproject.toml ./
RUN pip install -e .

# Create non-root user
RUN useradd --create-home --shell /bin/bash monitor

# Copy application code
COPY --chown=monitor:monitor . .

# Switch to non-root user
USER monitor

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD monitor-setup --help || exit 1

# Default command
ENTRYPOINT ["monitor-gui"]

# Development stage
FROM base as development
USER root
RUN pip install -e .[dev]
USER monitor
CMD ["--help"]