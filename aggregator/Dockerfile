FROM python:3.11-slim

# Gunakan non-root user untuk security best practice
RUN useradd -m -s /bin/bash appuser
WORKDIR /app

# Copy requirements dan install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code aplikasi
COPY . .

# Set permission dan jalankan via appuser
RUN chown -R appuser:appuser /app
USER appuser

# Default CMD adalah uvicorn (API Server)
# (Dapat ditimpa oleh command "python consumer.py" di docker-compose.yml untuk worker)
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]
