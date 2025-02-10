# Base image with CUDA support
FROM nvidia/cuda:12.1.0-cudnn8-runtime-ubuntu22.04

# Set working directory
WORKDIR /app

# Install dependencies
RUN apt-get update && apt-get install -y \
    python3 python3-pip git wget unzip && \
    pip3 install --upgrade pip && \
    pip3 install torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cu121 && \
    pip3 install git+https://github.com/hiyouga/LLaMA-Factory.git

# Copy files
COPY entrypoint.sh /app/entrypoint.sh
COPY model_train.jsonl /app/model_train.jsonl
RUN chmod +x /app/entrypoint.sh

# Define entrypoint
ENTRYPOINT ["/app/entrypoint.sh"]
