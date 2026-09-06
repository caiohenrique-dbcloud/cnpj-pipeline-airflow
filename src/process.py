"""
Camada SILVER (Processing): limpeza, filtro e cruzamento dos dados.

Por que DuckDB aqui?
  Os arquivos de Estabelecimentos somados passam de vários GB descompactados.
  Carregar tudo em memória com pandas não escala bem numa máquina comum.
  DuckDB processa os CSVs em streaming (out-of-core), com SQL, sem precisar
  de um cluster — ótimo custo-benefício para um MVP como este. Em um cenário
  de produção com múltiplos meses/UFs, o próximo passo natural seria migrar
  esse mesmo SQL para PySpark, mantendo a lógica de negócio idêntica.

O que essa camada faz:
  1. Descompacta os .zip da bronze em CSV (a RFB entrega os arquivos zipados).
  2. Lê os CSVs com DuckDB (sem cabeçalho -> aplicamos os nomes de config.py).
  3. Filtra: situacao_cadastral = Ativa E município = São Paulo.
  4. Faz o join com Municipios para traduzir o código -> nome do município.
  5. Grava o resultado filtrado em Parquet particionado por ano/mês (silver).
"""
import logging
import shutil
import zipfile
from pathlib import Path

import duckdb

from . import config
from .storage import Storage

logger = logging.getLogger(__name__)

TMP_DIR = config.ROOT_DIR / ".tmp_extract"


def _extract_zip(zip_path: Path, extract_dir: Path) -> list[Path]:
    extract_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path) as zf:
        zf.extractall(extract_dir)
        return [extract_dir / name for name in zf.namelist()]


def _transcode_latin1_to_utf8(path: Path) -> Path:
    """
    Os CSVs da RFB vêm em latin-1. A versão do DuckDB usada aqui não aceita
    o parâmetro `encoding` em read_csv_auto, então convertemos o arquivo para
    utf-8 antes de ler. Feito em streaming (não carrega o arquivo inteiro em
    memória), pois esses CSVs podem passar de 1GB.
    """
    tmp_path = path.with_name(path.name + ".utf8")
    with open(path, "r", encoding="latin-1", newline="") as src, \
         open(tmp_path, "w", encoding="utf-8", newline="") as dst:
        shutil.copyfileobj(src, dst)
    tmp_path.replace(path)
    return path


def run_silver_processing(year: int, month: int, storage: Storage, force: bool = False, mock: bool = False) -> str:
    """
    Processa a camada silver para o período dado.
    Retorna o path (local ou s3://) do parquet resultante.
    """
    prefix = config.build_prefix(year, month, mock=mock)
    silver_key = f"{prefix}/estabelecimentos_sp_ativos.parquet"

    if not force and storage.exists(config.BUCKET_SILVER, silver_key):
        logger.info("Silver já processado para %s, pulando.", prefix)
        return storage.resolve_path(config.BUCKET_SILVER, silver_key)

    extract_dir = TMP_DIR / prefix
    csv_estabelecimentos: list[Path] = []
    csv_municipios: list[Path] = []

    # Baixa da bronze (MinIO ou local) para um diretório temporário e descompacta.
    import re as _re

    bronze_root = config.LOCAL_DATA_DIR / config.BUCKET_BRONZE / prefix
    if storage.use_minio:
        # Lista objetos do bucket bronze para esse prefixo.
        objects = storage.client.list_objects(config.BUCKET_BRONZE, prefix=f"{prefix}/", recursive=True)
        filenames = [Path(obj.object_name).name for obj in objects]
    else:
        filenames = [p.name for p in bronze_root.glob("*.zip")]

    for filename in filenames:
        key = f"{prefix}/{filename}"
        local_zip = TMP_DIR / "zips" / prefix / filename
        storage.download_to(config.BUCKET_BRONZE, key, local_zip)
        extracted = _extract_zip(local_zip, extract_dir)

        if filename.startswith("Estabelecimentos"):
            for csv_path in extracted:
                _transcode_latin1_to_utf8(csv_path)
            csv_estabelecimentos.extend(extracted)
        elif filename.startswith("Municipios"):
            for csv_path in extracted:
                _transcode_latin1_to_utf8(csv_path)
            csv_municipios.extend(extracted)

    if not csv_estabelecimentos or not csv_municipios:
        raise RuntimeError(f"Arquivos CSV ausentes após extração para {prefix}.")

    silver_local_path = TMP_DIR / prefix / "estabelecimentos_sp_ativos.parquet"
    silver_local_path.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    storage.duckdb_configure(con)

    estab_names_sql = "[" + ", ".join(f"'{name}'" for name in config.COLUNAS_ESTABELECIMENTOS) + "]"
    munic_names_sql = "[" + ", ".join(f"'{name}'" for name in config.COLUNAS_MUNICIPIOS) + "]"
    estab_files_sql = ", ".join(f"'{p.as_posix()}'" for p in csv_estabelecimentos)
    munic_files_sql = ", ".join(f"'{p.as_posix()}'" for p in csv_municipios)

    query = f"""
    COPY (
        WITH estabelecimentos AS (
            SELECT *
            FROM read_csv_auto(
                [{estab_files_sql}],
                delim=';', header=false, quote='"',
                all_varchar=true, names={estab_names_sql}
            )
        ),
        municipios AS (
            SELECT *
            FROM read_csv_auto(
                [{munic_files_sql}],
                delim=';', header=false, quote='"',
                all_varchar=true, names={munic_names_sql}
            )
        )
        SELECT
            e.cnpj_basico,
            e.cnpj_ordem,
            e.cnpj_dv,
            e.identificador_matriz_filial,
            e.nome_fantasia,
            e.situacao_cadastral,
            e.data_situacao_cadastral,
            e.data_inicio_atividade,
            e.uf,
            m.nome_municipio
        FROM estabelecimentos e
        INNER JOIN municipios m ON e.codigo_municipio = m.codigo_municipio
        WHERE e.situacao_cadastral = '{config.SITUACAO_CADASTRAL_ATIVA}'
          AND m.nome_municipio = '{config.MUNICIPIO_ALVO}'
    ) TO '{silver_local_path.as_posix()}' (FORMAT PARQUET);
    """
    con.execute(query)
    con.close()

    return storage.upload_file(config.BUCKET_SILVER, silver_key, silver_local_path)
