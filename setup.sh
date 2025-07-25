#!/bin/bash
# Script para criar ambiente virtual e instalar dependências

set -e

# Cria o ambiente virtual se não existir
echo "[setup.sh] Criando ambiente virtual em .venv..."
python -m venv .venv

# Ativa o ambiente virtual
echo "[setup.sh] Ativando ambiente virtual..."
source .venv/bin/activate

# Atualiza pip
echo "[setup.sh] Atualizando pip..."
pip install --upgrade pip

# Instala dependências
echo "[setup.sh] Instalando dependências do requirements.txt..."
pip install -r requirements.txt

echo "[setup.sh] Ambiente pronto! Para ativar depois, use: source .venv/bin/activate"
