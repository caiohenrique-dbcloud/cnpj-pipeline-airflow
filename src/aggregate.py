"""
Camada GOLD (Curated): agregação final para consumo analítico.

Responde diretamente à pergunta de negócio:
  "Quantas filiais vs. matrizes com Situação Cadastral 'Ativa' existem
   na cidade de São Paulo?"

Gera:
  - Uma tabela agregada (Parquet + CSV) com a contagem por Matriz/Filial.
  - Um gráfico de barras (PNG) para visualização imediata.
"""
import logging
from pathlib import Path

import duckdb
import matplotlib

matplotlib.use("Agg")  # não precisa de display gráfico (roda em container/CI)
import matplotlib.pyplot as plt

from . import config
from .storage import Storage

logger = logging.getLogger(__name__)

TMP_DIR = config.ROOT_DIR / ".tmp_gold"


def run_gold_aggregation(year: int, month: int, storage: Storage, force: bool = False, mock: bool = False) -> dict:
    prefix = config.build_prefix(year, month, mock=mock)
    gold_csv_key = f"{prefix}/matriz_vs_filial_sp.csv"
    gold_png_key = f"{prefix}/matriz_vs_filial_sp.png"

    if not force and storage.exists(config.BUCKET_GOLD, gold_csv_key):
        logger.info("Gold já existe para %s, pulando agregação.", prefix)

    silver_key = f"{prefix}/estabelecimentos_sp_ativos.parquet"
    local_silver = TMP_DIR / prefix / "silver.parquet"
    storage.download_to(config.BUCKET_SILVER, silver_key, local_silver)

    con = duckdb.connect()
    result = con.execute(
        f"""
        SELECT
            CASE identificador_matriz_filial
                WHEN '1' THEN 'Matriz'
                WHEN '2' THEN 'Filial'
                ELSE 'Desconhecido'
            END AS tipo,
            COUNT(*) AS quantidade
        FROM read_parquet('{local_silver.as_posix()}')
        GROUP BY 1
        ORDER BY 2 DESC
        """
    ).fetchdf()
    con.close()

    local_csv = TMP_DIR / prefix / "matriz_vs_filial_sp.csv"
    local_csv.parent.mkdir(parents=True, exist_ok=True)
    result.to_csv(local_csv, index=False)

    total = int(result["quantidade"].sum())
    logger.info("Resultado %s -> %s | total=%d", prefix, result.to_dict("records"), total)

    # --- Gráfico ---
    local_png = TMP_DIR / prefix / "matriz_vs_filial_sp.png"
    fig, ax = plt.subplots(figsize=(6, 4))
    bars = ax.bar(result["tipo"], result["quantidade"], color=["#1f77b4", "#ff7f0e"])
    ax.set_title(f"Estabelecimentos Ativos em São Paulo — {prefix}\nMatriz vs. Filial")
    ax.set_ylabel("Quantidade")
    for bar in bars:
        height = bar.get_height()
        ax.annotate(f"{int(height):,}".replace(",", "."), (bar.get_x() + bar.get_width() / 2, height),
                    ha="center", va="bottom")
    fig.tight_layout()
    fig.savefig(local_png, dpi=150)
    plt.close(fig)

    csv_path = storage.upload_file(config.BUCKET_GOLD, gold_csv_key, local_csv)
    png_path = storage.upload_file(config.BUCKET_GOLD, gold_png_key, local_png)

    return {"table": result, "total": total, "csv_path": csv_path, "png_path": png_path}
