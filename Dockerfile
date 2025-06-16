# Dockerfile
FROM python:3.11-slim

# install ffmpeg
RUN apt-get update && \
    apt-get install -y ffmpeg && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy code in config
COPY requirements.txt ai_prompt.txt config.json pmda.py ./
COPY static/ ./static/

# Install python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Expose the default port
EXPOSE 5005

# Start the server in verbose mode and serve mode (to enable webui)
ENTRYPOINT ["python3", "pmda.py", "--serve", "--verbose"]
