FROM python:3.11-slim

# Optional but speeds up pandas builds from wheels
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential ca-certificates && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# default port most container hosts use if none provided
ENV PORT=7860
EXPOSE 7860

# use shell form so $PORT is expanded
CMD bash -lc "gunicorn -k gthread -w 2 -t 120 -b 0.0.0.0:${PORT:-7860} app:app"
