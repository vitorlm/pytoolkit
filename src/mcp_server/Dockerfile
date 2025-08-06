# Imagem base oficial com Python 3.13 (mais recente e segura)
FROM python:3.13-slim-bookworm

# Cria diretório de trabalho
WORKDIR /app

# Copia apenas os arquivos necessários inicialmente para cache eficiente
COPY requirements.txt .

# Atualiza pacotes do sistema operacional para mitigar CVEs da imagem base
RUN apt-get update && apt-get upgrade -y && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Instala dependências do Python (inclusive o MCP)
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt && \
    pip install --no-cache-dir mcp

# Copia o restante do código e pastas necessárias
COPY src/ src/
COPY data/ data/
COPY cache/ cache/
COPY logs/ logs/
COPY output/ output/

# Garante que as pastas existem mesmo se não vierem no contexto
RUN mkdir -p cache logs output data

# Define o PYTHONPATH para permitir imports relativos
ENV PYTHONPATH=/app

# Expondo a porta 8000 (caso necessário no futuro)
EXPOSE 8000

# Health check robusto para garantir que o módulo principal está carregando
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "from src.mcp_server.management_mcp_server import ManagementMCPServer; print('OK')" || exit 1

# Comando padrão de execução
CMD ["python", "src/mcp_server/management_mcp_server.py"]
