########################
# 1️⃣ builder stage
########################
FROM python:3.10-slim-bullseye AS builder

# System packages required for lxml, cryptography & Playwright
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive \
    apt-get install -y --no-install-recommends \
        build-essential gcc libxml2-dev libxslt1-dev libffi-dev \
        curl git && \
    rm -rf /var/lib/apt/lists/*

# Keep pip cache outside final image
WORKDIR /install

# Copy requirements early to maximise Docker layer caching
COPY requirements.txt .

RUN pip install --upgrade pip && \
    pip wheel --no-cache-dir --wheel-dir /install/wheels -r requirements.txt

# ----------------------
# Playwright browsers
# ----------------------
#  (chromium only – adds ~200 MB; if you want all three, change the arg)
RUN pip install --no-cache-dir playwright==1.40.0 && \
    python -m playwright install --with-deps chromium

########################
# 2️⃣ runtime stage
########################
FROM python:3.10-slim-bullseye

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    # Default file paths used by the app
    PROJECT_ROOT=/app \
    PYTHONPATH=/app \
    SCRAPY_SETTINGS_MODULE=crawler.spider_project.settings \
    # Make sure scripts are in PATH
    PATH="/app/.local/bin:$PATH"

# System dependencies for runtime
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        libxml2 libxslt1.1 \
        # For playwright
        libnss3 \
        libnspr4 \
        libatk1.0-0 \
        libatk-bridge2.0-0 \
        libcups2 \
        libdbus-1-3 \
        libdrm2 \
        libxkbcommon0 \
        libxcomposite1 \
        libxdamage1 \
        libxfixes3 \
        libxrandr2 \
        libasound2 \
        libpango-1.0-0 \
        libcairo2 && \
    rm -rf /var/lib/apt/lists/*

# ── Create non-root user ──
RUN adduser --disabled-password --gecos "" appuser

WORKDIR /app

# Copy wheel files and install
COPY --from=builder /install/wheels /wheels
RUN pip install --no-cache-dir --no-index --find-links=/wheels /wheels/* && \
    pip install scrapy && \
    # Make scrapy globally available
    ln -sf /usr/local/bin/scrapy /usr/bin/scrapy

# Copy source code last (so code changes don't bust early layers)
COPY --chown=appuser:appuser . /app

# Create data directories
RUN mkdir -p /app/data/logs /app/data/html && \
    chmod -R 777 /app/data

# Install the package in development mode
RUN pip install -e .

# Switch to non-root user
USER appuser

# Health-check script
HEALTHCHECK CMD python -c "import socket,sys; s=socket.socket(); s.settimeout(2); s.connect(('127.0.0.1', 11300)); sys.exit(0)"

# For development with manual commands, use this:
ENTRYPOINT ["tail", "-f", "/dev/null"]
