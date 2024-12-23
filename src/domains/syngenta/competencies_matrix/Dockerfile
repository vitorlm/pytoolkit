FROM ollama/ollama:latest

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

COPY Modelfile /Modelfile

ENTRYPOINT ["/entrypoint.sh"]