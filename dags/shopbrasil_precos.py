"""
### Panorama de preços por categoria — ShopBrasil

Substitui o script agendado via cron que alimentava o painel de preços.
Todo dia às 06:00 (horário de Brasília) o pipeline coleta os produtos da API
de catálogo, calcula as métricas de preço por categoria e grava o resultado
na base analítica (PostgreSQL).
"""

import logging
from datetime import timedelta

import pendulum
from airflow.sdk import dag, task, task_group

from shopbrasil import banco, metricas
from shopbrasil.api import ErroApiCatalogo, buscar_produtos_api
from shopbrasil.callbacks import ao_concluir_task, ao_falhar_task, ao_retentar_task
from shopbrasil.config import TIMEZONE

log = logging.getLogger(__name__)


@dag(
    dag_id="shopbrasil_precos_categoria",
    description="Panorama diário de preços por categoria para o time de pricing",
    schedule="0 6 * * *",
    start_date=pendulum.datetime(2026, 11, 1, tz=TIMEZONE),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "time-dados",
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["shopbrasil", "precos", "ecommerce"],
    doc_md=__doc__,
)
def shopbrasil_precos_categoria():
    @task_group(group_id="ingestao")
    def ingestao():
        """Linear: preparar tabelas -> buscar produtos -> agrupar por categoria."""

        @task
        def preparar_tabelas() -> None:
            banco.criar_tabelas()

        @task(
            retries=4,
            retry_delay=timedelta(seconds=20),
            retry_exponential_backoff=True,
            max_retry_delay=timedelta(minutes=5),
            on_success_callback=ao_concluir_task,
            on_retry_callback=ao_retentar_task,
            on_failure_callback=ao_falhar_task,
        )
        def buscar_produtos() -> list[dict]:
            try:
                return buscar_produtos_api()
            except ErroApiCatalogo:
                log.exception("Não foi possível buscar os produtos; o Airflow tentará novamente")
                raise

        @task
        def agrupar_por_categoria(produtos: list[dict]) -> list[dict]:
            grupos = metricas.agrupar_por_categoria(produtos)
            log.info("Categorias encontradas: %s", [g["categoria"] for g in grupos])
            return grupos

        produtos = buscar_produtos()
        preparar_tabelas() >> produtos
        return agrupar_por_categoria(produtos)

    ingestao()


shopbrasil_precos_categoria()
