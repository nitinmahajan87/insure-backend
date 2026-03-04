# Use an official Python runtime as a parent image
FROM python:3.11-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# Set the working directory in the container
WORKDIR /app

# Install system dependencies (needed for psycopg2 and other tools)
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
# We do this before copying the whole project to leverage Docker cache
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY . .

# Create the outbound_files directory inside the container
RUN mkdir -p /app/outbound_files

# Make the entrypoint executable
RUN chmod +x /app/entrypoint.sh

# Default command (overridden by docker-compose for worker/beat services)
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]