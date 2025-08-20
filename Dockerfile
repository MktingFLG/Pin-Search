# Use Python 3.12
FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Install system deps (important for pandas, numpy, etc.)
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

# Copy project files
COPY . .

# Run your app (adjust to your entrypoint)
CMD ["gunicorn", "app:app"]
