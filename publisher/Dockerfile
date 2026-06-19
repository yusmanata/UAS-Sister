FROM python:3.11-slim

RUN useradd -m -s /bin/bash appuser
WORKDIR /app

RUN pip install --no-cache-dir httpx==0.27.0 asyncio

COPY . .

RUN chown -R appuser:appuser /app
USER appuser

CMD ["python", "main.py"]
