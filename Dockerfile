FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PYTHONUNBUFFERED=1

EXPOSE 7860

CMD ["python", "-m", "app.main"]

# Railway Nixpacks start command (if not using Dockerfile)
