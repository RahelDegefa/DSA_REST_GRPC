# Use a lightweight official Python 3.13 image
FROM python:3.13-slim

# Prevent Python from writing .pyc files and buffer stdout/stderr for real-time logs
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV SERVICE_NAME=items-grpc-service
ENV GRPC_PORT=50051

WORKDIR /app

# Copy requirements first to leverage Docker layer caching
COPY requirements.txt .

# Fix 1: Added spaces for pip install command
RUN python -m pip install --no-cache-dir -r requirements.txt

# Fix 2: Added spaces for useradd flags
RUN useradd --create-home --shell /usr/sbin/nologin appuser

# Fix 3: Added space after COPY
COPY --chown=appuser:appuser . .

USER appuser

EXPOSE 50051

CMD ["python", "server.py"]