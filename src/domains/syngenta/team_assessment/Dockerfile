FROM ollama/ollama:latest

# Copy entrypoint script
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Copy Modelfile
COPY Modelfile /Modelfile

# The ENTRYPOINT instruction remains the same, executing your entrypoint script
ENTRYPOINT ["/entrypoint.sh"]