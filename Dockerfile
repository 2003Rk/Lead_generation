FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    git \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements first (for better caching)
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install gunicorn

# Install Playwright and browsers
RUN playwright install-deps
RUN playwright install

# Copy application code
COPY . .

# Create necessary directories
RUN mkdir -p data/exports logs

# Expose port
EXPOSE 8080

# Set environment variables
ENV FLASK_ENV=production
ENV PYTHONUNBUFFERED=1

# Run the application
CMD ["gunicorn", "-w", "4", "-b", "0.0.0.0:8080", "webapp:app"]