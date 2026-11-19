FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# Create non-root user appuser (uid 10001) and create /state owned by appuser
RUN groupadd -g 10001 appuser && \
    useradd -u 10001 -g appuser -s /bin/sh -m appuser && \
    mkdir -p /state && \
    chown -R appuser:appuser /state

WORKDIR /app

# Copy requirements first and pip install
COPY services/coordinator/requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy common library and pip install it
COPY common/ /app/common/
RUN pip install --no-cache-dir -e /app/common

# Copy service code
COPY services/coordinator/coordinator/ /app/coordinator/
RUN chown -R appuser:appuser /app

VOLUME /state

USER appuser

EXPOSE 8000

HEALTHCHECK --interval=5s --timeout=3s --start-period=5s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health/live')" || exit 1

CMD ["uvicorn", "coordinator.main:app", "--host", "0.0.0.0", "--port", "8000"]
