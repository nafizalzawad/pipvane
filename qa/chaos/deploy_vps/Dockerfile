# ==============================================================================
# Pipvane v3: 30-Day Soak & Chaos Testing Container
# ==============================================================================
FROM python:3.11-slim

# System dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    procps \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy dependency requirements & install
COPY qa/ /app/qa/
COPY engine/ /app/engine/

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir psutil pytest pytest-asyncio flake8

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# Output directory for daily ban_risk_telemetry.csv
VOLUME ["/app/telemetry"]

# Default entrypoint: runs Soak Orchestrator for 30 days (720 hours)
ENTRYPOINT ["python", "qa/chaos/soak_orchestrator.py", "--duration-hours", "720", "--output-dir", "/app/telemetry"]
