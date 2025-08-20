# Use Python 3.12 slim (stable + supported by pandas/numpy)
FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Install system dependencies needed for pandas/numpy
RUN apt-get update && apt-get install -y \
    build-essential \
    gcc \
    g++ \
    git \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements.txt .

# Install Python dependencies
RUN pip install --upgrade pip setuptools wheel
RUN pip install -r requirements.txt

# Copy the rest of your code
COPY . .

# Don’t set CMD here — Render will override with startCommand
