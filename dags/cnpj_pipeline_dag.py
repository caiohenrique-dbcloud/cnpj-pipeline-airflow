"""
DAG do Airflow para o Pipeline CNPJ — Matriz vs. Filial em São Paulo.

Em vez de rodar tudo num único script (como fazíamos em `src/pipeline.py`),
aqui cada camada (Bronze, Silver, Gold) vira uma TAREFA separada. Isso traz
vantagens reais de orquestração:

  - Se só a camada Silver falhar, o Airflow reprocessa só ela (não precisa
    baixar tudo de novo) — graças à idempotência que já existia no projeto.
  - Retries automáticos por tarefa, com espera entre tentativas.
  - Agendamento (aqui mensal, no dia 20 de cada mês, horário configurável).
  - Visualização gráfica do progresso na interface web do Airflow.

As funções chamadas aqui são EXATAMENTE as mesmas de src/ingest.py,
src/process.py e src/aggregate.py — não duplicamos lógica de negócio,
só trocamos "quem chama" essas funções (antes era um script Python direto,
agora é o Airflow).
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

default_args = {
    "owner": "data-engineering",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
}


def _task_bronze(**context):
    from src import ingest
    from src.storage import Storage

    conf = context["dag_run"].conf or {}
    year = int(conf.get("year", 2025))
    month = int(conf.get("month", 12))

    # O Airflow entrega campos de formulário deixados em branco como texto
    # vazio ("") em vez de None — sem esse tratamento, isso quebra o "slice"
    # da lista de arquivos em ingest.py (TypeError: slice indices must be
    # integers or None). Convertemos explicitamente para int, ou None se
    # vazio/ausente.
    sample_raw = conf.get("sample")
    sample = int(sample_raw) if sample_raw not in (None, "") else None

    # Mesmo tratamento de tipo do "sample": o Airflow pode entregar um
    # checkbox desmarcado como False (ok) ou, em alguns casos, como texto —
    # normalizamos para garantir um bool de verdade.
    mock_raw = conf.get("mock", False)
    mock = mock_raw in (True, "true", "True", "1", 1)

    storage = Storage()
    result = ingest.run_bronze_ingestion(
        year, month, storage, force=False, max_estab_files=sample, mock=mock
    )
    # Passa o ano/mês REALMENTE resolvido (pode ter caído para um mês
    # anterior, por causa do fallback) para as próximas tarefas via XCom.
    context["ti"].xcom_push(key="resolved_year", value=result["year"])
    context["ti"].xcom_push(key="resolved_month", value=result["month"])
    # Propaga também se essa execução é mock — CRÍTICO: sem isso, Silver e
    # Gold usariam o prefixo "real" mesmo para dados fictícios, colidindo
    # com uma execução real do mesmo mês (ver build_prefix em config.py).
    context["ti"].xcom_push(key="mock", value=mock)


def _task_silver(**context):
    from src import process
    from src.storage import Storage

    ti = context["ti"]
    year = ti.xcom_pull(task_ids="bronze_ingestao", key="resolved_year")
    month = ti.xcom_pull(task_ids="bronze_ingestao", key="resolved_month")
    mock = ti.xcom_pull(task_ids="bronze_ingestao", key="mock")

    storage = Storage()
    process.run_silver_processing(year, month, storage, force=False, mock=mock)


def _task_gold(**context):
    from src import aggregate
    from src.storage import Storage

    ti = context["ti"]
    year = ti.xcom_pull(task_ids="bronze_ingestao", key="resolved_year")
    month = ti.xcom_pull(task_ids="bronze_ingestao", key="resolved_month")
    mock = ti.xcom_pull(task_ids="bronze_ingestao", key="mock")

    storage = Storage()
    result = aggregate.run_gold_aggregation(year, month, storage, force=False, mock=mock)
    logger.info("Resultado final: %s", result["table"].to_dict("records"))


with DAG(
    dag_id="cnpj_pipeline_matriz_filial_sp",
    description="Pipeline CNPJ: Matriz vs Filial ativas em São Paulo (Bronze -> Silver -> Gold)",
    default_args=default_args,
    schedule="0 6 20 * *",  # todo dia 20 do mês, às 6h — RFB costuma publicar por essa data
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["cnpj", "receita-federal", "medallion"],
    params={"year": 2025, "month": 12, "sample": None, "mock": False},
) as dag:

    bronze = PythonOperator(
        task_id="bronze_ingestao",
        python_callable=_task_bronze,
    )

    silver = PythonOperator(
        task_id="silver_processamento",
        python_callable=_task_silver,
    )

    gold = PythonOperator(
        task_id="gold_agregacao",
        python_callable=_task_gold,
    )

    bronze >> silver >> gold
