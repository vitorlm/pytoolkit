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
echo "Pullingdeepseek-r1.5 model with 'ollama pull deepseek-r1.5'"
ollama run deepseek-r1:1.5b 2>&1

echo "Creating TalentForgeAI model with 'ollama create TalentForgeAI -f Modelfile'"
ollama create TalentForgeAI -f Modelfile 2>&1
echo "Model created"

ollama run TalentForgeAI 2>&1

# Bring the ollama serve logs back to the foreground
tail -f /var/log/ollama.log