# Usa uma imagem base do Python
FROM python:3.9-slim

# Define o diretório de trabalho dentro do container
WORKDIR /app

# Copia o arquivo de requisitos para o diretório de trabalho
COPY requirements.txt .

# Instala as dependências listadas no requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copia o script Python para o diretório de trabalho
COPY updateRanking.py .

# Executa o script quando o container iniciar
CMD ["python", "updateRanking.py"]
