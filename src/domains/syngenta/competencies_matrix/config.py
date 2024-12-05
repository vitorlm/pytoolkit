import os
from dotenv import load_dotenv

# Carregar as variáveis de ambiente do arquivo .env
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), ".env"))

class Config:
    HOST = os.getenv("OLLAMA_HOST", "http://localhost:11434")
    MODEL = os.getenv("OLLAMA_MODEL", "llama2")
    MAX_TOKENS = int(os.getenv("OLLAMA_MAX_TOKENS", 256))
    TEMPERATURE = float(os.getenv("OLLAMA_TEMPERATURE", 0.7))

