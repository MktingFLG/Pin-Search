# --- Stage 1: Builder (compile heavy deps like numpy/pandas) ---
FROM python:3.12-slim AS builder

WORKDIR /app

# Keep BLAS/NumExpr from spawning extra threads (saves RAM & CPU)
ENV OMP_NUM_THREADS=1 \
    OPENBLAS_NUM_THREADS=1 \
    MKL_NUM_THREADS=1 \
    NUMEXPR_NUM_THREADS=1 \
    PYTHONOPTIMIZE=1

    
# Install build tools only in this stage
RUN apt-get update && apt-get install -y \
    build-essential \
    gcc \
    g++ \
    git \
    && rm -rf /var/lib/apt/lists/*

# Upgrade pip/setuptools/wheel to fixed versions (avoid random breakage)
RUN pip install --upgrade pip==24.0 setuptools==70.0.0 wheel==0.43.0

# Install dependencies into a /wheels dir
COPY requirements.txt .
RUN pip wheel --wheel-dir=/wheels -r requirements.txt


# --- Stage 2: Runtime (lighter final image) ---
FROM python:3.12-slim

WORKDIR /app

# Install only minimal runtime deps (no compilers here)
RUN apt-get update && apt-get install -y \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy prebuilt wheels from builder
COPY --from=builder /wheels /wheels
RUN pip install --no-cache-dir /wheels/*

# Copy app code
COPY . .

# Default command (Render overrides this with render.yaml anyway)
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "10000"]
