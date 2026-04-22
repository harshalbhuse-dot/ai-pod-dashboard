FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY feedback_api.py .

# Cloud Run uses PORT env variable
ENV PORT=8080

# Run the application
CMD ["python", "-m", "uvicorn", "feedback_api:app", "--host", "0.0.0.0", "--port", "8080"]
