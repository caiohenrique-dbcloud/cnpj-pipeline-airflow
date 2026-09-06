"""
Ponto de entrada do pipeline.

Uso:
    python -m src.pipeline --year 2025 --month 12
    python -m src.pipeline --year 2025 --month 12 --force   # reprocessa mesmo se já existir

Se o período pedido ainda não tiver sido publicado pela Receita Federal, o
pipeline automaticamente recua para o mês publicado mais recente (ver
ingest.resolve_reference_period), atendendo ao requisito de negócio
"Dezembro de 2025 ou o mês mais recente disponível".
"""
import argparse
import logging
import sys

from . import aggregate, ingest, process
from .storage import Storage

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logger = logging.getLogger("pipeline")


def parse_args():
    parser = argparse.ArgumentParser(description="Pipeline CNPJ - Matriz vs Filial em São Paulo")
    parser.add_argument("--year", type=int, required=True, help="Ano de referência, ex: 2025")
    parser.add_argument("--month", type=int, required=True, help="Mês de referência (1-12), ex: 12")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Reprocessa todas as camadas mesmo que os dados já existam (ignora idempotência)",
    )
    parser.add_argument(
        "--sample",
        type=int,
        default=None,
        metavar="N",
        help=(
            "Modo de teste rápido: baixa apenas N arquivos de Estabelecimentos "
            "(de ~10 disponíveis), em vez do mês inteiro. Ex: --sample 1"
        ),
    )
    parser.add_argument(
        "--mock",
        action="store_true",
        help=(
            "Gera dados sintéticos localmente, sem acessar a internet. Útil "
            "para validar a pipeline (ou o Airflow) quando a fonte de dados "
            "ou a conexão estiverem instáveis."
        ),
    )
    return parser.parse_args()


def run(year: int, month: int, force: bool = False, sample: int | None = None, mock: bool = False) -> dict:
    storage = Storage()

    logger.info("=== [1/3] BRONZE: ingestão ===")
    bronze_meta = ingest.run_bronze_ingestion(
        year, month, storage, force=force, max_estab_files=sample, mock=mock
    )
    resolved_year, resolved_month = bronze_meta["year"], bronze_meta["month"]
    if (resolved_year, resolved_month) != (year, month):
        logger.warning(
            "Período %04d-%02d indisponível na fonte. Usando o mais recente publicado: %04d-%02d",
            year, month, resolved_year, resolved_month,
        )

    logger.info("=== [2/3] SILVER: processamento e filtro ===")
    silver_path = process.run_silver_processing(resolved_year, resolved_month, storage, force=force, mock=mock)
    logger.info("Silver gravado em: %s", silver_path)

    logger.info("=== [3/3] GOLD: agregação final ===")
    gold_result = aggregate.run_gold_aggregation(resolved_year, resolved_month, storage, force=force, mock=mock)

    logger.info("Pipeline concluído com sucesso para %04d-%02d.", resolved_year, resolved_month)
    logger.info("Tabela final:\n%s", gold_result["table"].to_string(index=False))
    logger.info("Total de estabelecimentos ativos em SP: %d", gold_result["total"])

    return gold_result


if __name__ == "__main__":
    args = parse_args()
    try:
        run(args.year, args.month, force=args.force, sample=args.sample, mock=args.mock)
    except Exception:
        logger.exception("Pipeline falhou.")
        sys.exit(1)
