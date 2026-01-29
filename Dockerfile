FROM python:3.12-slim

WORKDIR /app

# Install Python deps first (better caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy your repo into the container
COPY . .

# Run the backup pipeline
CMD ["python", "run_backup.py"]
