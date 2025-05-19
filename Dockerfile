# Use the official Python image from the Docker Hub
FROM python:3.11-slim

# Set work directory
WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt .
RUN pip install --upgrade pip setuptools
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire application
COPY . .

# Use the PORT environment variable provided by Cloud Run
ENV PORT=8080

# Expose the port
EXPOSE ${PORT}

# Use a shell script to start the application
CMD ["python", "./main.py"] 