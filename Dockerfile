FROM python:3.12-slim

WORKDIR /app

# docker.io: CLI talks to host Docker via mounted /var/run/docker.sock (no daemon in this image).
RUN apt-get update && apt-get install -y --no-install-recommends \
    docker.io \
    curl \
    && rm -rf /var/lib/apt/lists/* \
    && docker --version

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .
COPY templates/ templates/

EXPOSE 5000

CMD ["python", "app.py"]
