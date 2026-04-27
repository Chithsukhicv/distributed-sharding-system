FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Command is overridden in docker-compose.yml for backend vs central
CMD ["python", "central_server.py"]
