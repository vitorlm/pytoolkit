#!/bin/bash

# Serve using ollama
echo "Starting 'ollama serve'"
ollama serve > /var/log/ollama.log 2>&1 &

# Capture the PID of the background process
OLLAMA_PID=$!

# Wait for the ollama server to be ready
echo "Waiting for 'ollama serve' to be ready..."
while ! ollama list > /dev/null 2>&1; do
  sleep 1
done

echo "'ollama' is now available!"

# Pull the llama3.2 model using ollama
echo "Pulling llama3.2 model with 'ollama pull llama3.2'"
ollama pull llama3.2 2>&1

# Bring the ollama serve logs back to the foreground
tail -f /var/log/ollama.log