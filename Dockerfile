FROM python:3.12-slim

WORKDIR /app

ENV PYTHONPATH=/app/src

# requirements-api.txt does `-r requirements.txt`, so both are needed.
COPY requirements.txt requirements-api.txt ./
RUN pip install --no-cache-dir -r requirements-api.txt

COPY src ./src
COPY data/02_silver_tables/silver.db ./data/02_silver_tables/silver.db

RUN useradd --create-home --shell /bin/bash wicketgraph \
    && mkdir -p /app/data/02_silver_tables \
    && chown -R wicketgraph:wicketgraph /app

USER wicketgraph

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/health', timeout=3).read()" || exit 1

CMD ["sh", "-c", "PYTHONPATH=/app/src uvicorn api.app:app --host 0.0.0.0 --port 8000"]
