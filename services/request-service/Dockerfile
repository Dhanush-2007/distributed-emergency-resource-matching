FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# Create non-root user appuser (uid 10001)
RUN groupadd -g 10001 appuser && \
    useradd -u 10001 -g appuser -s /bin/sh -m appuser

WORKDIR /app

# Copy requirements first and pip install
COPY services/request-service/requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy common library and pip install it
COPY common/ /app/common/
RUN pip install --no-cache-dir -e /app/common

# Copy service code
COPY services/request-service/request_service/ /app/request_service/
RUN chown -R appuser:appuser /app

USER appuser

EXPOSE 8001

HEALTHCHECK --interval=5s --timeout=3s --start-period=5s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8001/health')" || exit 1

CMD ["uvicorn", "request_service.main:app", "--host", "0.0.0.0", "--port", "8001"]
