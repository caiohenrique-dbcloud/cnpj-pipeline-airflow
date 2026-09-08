"""
Configurações centrais do pipeline.

Todas as opções podem ser sobrescritas por variáveis de ambiente (.env),
o que facilita rodar o mesmo código em local / Docker / CI sem alterar código.
"""
import os
from pathlib import Path

# --- Diretório raiz do projeto -------------------------------------------------
ROOT_DIR = Path(__file__).resolve().parent.parent

# --- Fonte de dados (Receita Federal) ------------------------------------------
# A RFB migrou o domínio algumas vezes nos últimos anos. Mantemos uma lista de
# domínios candidatos e testamos em ordem — isso deixa a ingestão resiliente a
# mudanças de infraestrutura da fonte, sem precisar alterar código.
BASE_URL_CANDIDATES = [
    "https://dadosabertos.rfb.gov.br/CNPJ/dados_abertos_cnpj",
    "https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj",
]

# Mirror mantido pela Casa dos Dados (CDN Cloudflare, mesma fonte oficial).
# Serve de fallback quando os domínios oficiais acima estão instáveis/fora do ar
# (o que acontece com frequência nesse portal da RFB).
# Estrutura de pastas DIFERENTE dos domínios oficiais: em vez de "YYYY-MM/", usa
# "YYYY-MM-DD/" (data de publicação do mês), por isso é tratado à parte em ingest.py.
MIRROR_INDEX_URL = "https://dados-abertos-rf-cnpj.casadosdados.com.br/arquivos"

# --- Camadas do Data Lake (Medallion Architecture) ------------------------------
# USE_MINIO=true  -> grava em um bucket S3-compatível (MinIO)
# USE_MINIO=false -> grava em disco local, em ./data (útil para testar sem Docker)
USE_MINIO = os.getenv("USE_MINIO", "false").lower() == "true"

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"

BUCKET_BRONZE = os.getenv("BUCKET_BRONZE", "bronze")
BUCKET_SILVER = os.getenv("BUCKET_SILVER", "silver")
BUCKET_GOLD = os.getenv("BUCKET_GOLD", "gold")

LOCAL_DATA_DIR = ROOT_DIR / "data"

# --- Regras de negócio -----------------------------------------------------
# Códigos do layout oficial "LAYOUT_DADOS_ABERTOS_CNPJ.pdf" (RFB).
SITUACAO_CADASTRAL_ATIVA = "02"

# No arquivo de Estabelecimentos, a coluna "identificador_matriz_filial":
# 1 = Matriz | 2 = Filial
MATRIZ_FILIAL_MAP = {"1": "Matriz", "2": "Filial"}

MUNICIPIO_ALVO = "SAO PAULO"  # nome vem sem acento nos dados da RFB


def build_prefix(year: int, month: int, mock: bool = False) -> str:
    """
    Constrói o prefixo usado para nomear objetos no storage (bronze/silver/gold).

    Usar um prefixo diferente para dados mock ("MOCK-...") é essencial: sem
    isso, os arquivos fictícios do modo de teste ocupariam o MESMO endereço
    dos dados reais, e a idempotência (que verifica "esse arquivo já existe?")
    faria uma execução real subsequente pular o download de verdade, pensando
    que os dados já estavam lá — quando na verdade eram fictícios. Isso
    aconteceu de fato durante o desenvolvimento deste projeto (ver README,
    seção "Desafios enfrentados") e está documentado como lição aprendida.
    """
    base = f"{year:04d}-{month:02d}"
    return f"MOCK-{base}" if mock else base

# --- Nomes de colunas do arquivo de Estabelecimentos (sem cabeçalho no CSV) -----
# A RFB entrega os CSVs SEM header. A ordem das colunas é fixa e documentada
# no layout oficial. Mapeamos aqui pra deixar o código legível daqui pra frente.
COLUNAS_ESTABELECIMENTOS = [
    "cnpj_basico",
    "cnpj_ordem",
    "cnpj_dv",
    "identificador_matriz_filial",
    "nome_fantasia",
    "situacao_cadastral",
    "data_situacao_cadastral",
    "motivo_situacao_cadastral",
    "nome_cidade_exterior",
    "pais",
    "data_inicio_atividade",
    "cnae_fiscal_principal",
    "cnae_fiscal_secundaria",
    "tipo_logradouro",
    "logradouro",
    "numero",
    "complemento",
    "bairro",
    "cep",
    "uf",
    "codigo_municipio",
    "ddd1",
    "telefone1",
    "ddd2",
    "telefone2",
    "ddd_fax",
    "fax",
    "email",
    "situacao_especial",
    "data_situacao_especial",
]

COLUNAS_MUNICIPIOS = ["codigo_municipio", "nome_municipio"]

USE_S3 = os.getenv("USE_S3", "false").lower() == "true"
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "")
