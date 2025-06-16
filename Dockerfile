# Dockerfile
FROM python:3.11-slim

# Installer ffmpeg pour ffprobe
RUN apt-get update && \
    apt-get install -y ffmpeg && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copier le code et la config
COPY requirements.txt ai_prompt.txt config.json pmda.py ./
COPY static/ ./static/

# Installer les dépendances Python
RUN pip install --no-cache-dir -r requirements.txt

# Exposer le port Web UI
EXPOSE 5005

# Lancer le serveur en mode verbeux par défaut
ENTRYPOINT ["python3", "pmda.py", "--serve", "--verbose"]
