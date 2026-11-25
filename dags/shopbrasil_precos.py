"""
### Panorama de preços por categoria — ShopBrasil

Substitui o script agendado via cron que alimentava o painel de preços.
Todo dia às 06:00 (horário de Brasília) o pipeline coleta os produtos da API
de catálogo, calcula as métricas de preço por categoria e grava o resultado
na base analítica (PostgreSQL).
"""

import logging
from datetime import date, timedelta

import pendulum
from airflow.sdk import dag, get_current_context, task, task_group

from shopbrasil import banco, metricas
from shopbrasil.api import ErroApiCatalogo, buscar_produtos_api
from shopbrasil.callbacks import ao_concluir_task, ao_falhar_task, ao_retentar_task
from shopbrasil.config import POOL_CATEGORIAS, TIMEZONE
from shopbrasil.operators import ValidarProdutosOperator

log = logging.getLogger(__name__)


def _data_referencia() -> date:
    """Data (horário de Brasília) que a run representa.

    Runs agendadas usam a logical_date; runs manuais disparadas pela API podem
    não ter logical_date no Airflow 3, então usamos o run_after.
    """
    context = get_current_context()
    momento = context.get("logical_date") or context["dag_run"].run_after
    return pendulum.instance(momento).in_timezone(TIMEZONE).date()


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
        """Linear: preparar tabelas -> buscar -> validar -> agrupar por categoria."""

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
        validados = ValidarProdutosOperator(task_id="validar_produtos", produtos=produtos)
        return agrupar_por_categoria(validados.output)

    @task_group(group_id="analise")
    def analise(grupos):
        """Fan-out por categoria (Dynamic Task Mapping) e fan-in na consolidação."""

        @task(pool=POOL_CATEGORIAS, map_index_template="{{ categoria }}")
        def calcular_metricas(grupo: dict) -> dict:
            # Mostra o nome da categoria no lugar do índice na UI.
            get_current_context()["categoria"] = grupo["categoria"]
            return metricas.calcular_metricas(grupo["categoria"], grupo["precos"])

        @task
        def consolidar_metricas(resultados) -> list[dict]:
            linhas = metricas.consolidar(list(resultados))
            for linha in linhas:
                log.info(
                    "%-20s qtd=%-3d médio=%9.2f mín=%9.2f máx=%9.2f",
                    linha["categoria"],
                    linha["qtd_produtos"],
                    linha["preco_medio"],
                    linha["preco_minimo"],
                    linha["preco_maximo"],
                )
            return linhas

        return consolidar_metricas(calcular_metricas.expand(grupo=grupos))

    @task_group(group_id="persistencia")
    def persistencia(linhas):
        """Grava o resultado na base analítica."""

        @task
        def salvar_snapshot(linhas: list[dict], run_id=None) -> int:
            data_referencia = _data_referencia()
            total = banco.salvar_snapshot(data_referencia, linhas, run_id)
            log.info("Snapshot de %s gravado: %d categorias", data_referencia, total)
            return total

        @task
        def salvar_historico(linhas: list[dict], run_id=None) -> int:
            data_referencia = _data_referencia()
            total = banco.inserir_historico(data_referencia, linhas, run_id)
            log.info("Histórico de %s acrescentado: %d linhas", data_referencia, total)
            return total

        salvar_snapshot(linhas)
        salvar_historico(linhas)

    persistencia(analise(ingestao()))


shopbrasil_precos_categoria()
