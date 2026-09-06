FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/
COPY tests/ ./tests/

# Executa o pipeline por padrão; ano/mês podem ser sobrescritos no docker run/compose.
ENTRYPOINT ["python", "-m", "src.pipeline"]
CMD ["--year", "2025", "--month", "12"]
