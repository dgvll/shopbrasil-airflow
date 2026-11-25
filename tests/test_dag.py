"""Testes de integridade do DAG: carrega sem erros e segue os padrões do time."""

import pytest

from conftest import DAGS_DIR

try:
    from airflow.dag_processing.dagbag import DagBag
except ImportError:  # versões anteriores do Airflow
    from airflow.models.dagbag import DagBag

DAG_ID = "shopbrasil_precos_categoria"


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder=str(DAGS_DIR))


@pytest.fixture(scope="module")
def dag(dagbag):
    # Lê direto do que foi parseado, sem consultar o banco de metadados.
    return dagbag.dags[DAG_ID]


def test_dags_carregam_sem_erros(dagbag):
    assert dagbag.import_errors == {}
    assert DAG_ID in dagbag.dag_ids


def test_agendamento_diario_as_6h_de_brasilia(dag):
    assert dag.timetable.expression == "0 6 * * *"
    assert str(dag.timetable.timezone) == "America/Sao_Paulo"
    assert dag.timezone.name == "America/Sao_Paulo"
    assert dag.catchup is False


def test_tasks_organizadas_em_task_groups(dag):
    grupos = {task.task_group.group_id for task in dag.tasks}

    assert {"ingestao", "analise", "persistencia"} <= grupos


def test_topologia_linear_na_ingestao(dag):
    cadeia = [
        "ingestao.preparar_tabelas",
        "ingestao.buscar_produtos",
        "ingestao.validar_produtos",
        "ingestao.agrupar_por_categoria",
    ]
    for anterior, seguinte in zip(cadeia, cadeia[1:]):
        assert dag.get_task(anterior).downstream_task_ids == {seguinte}


def test_fan_out_mapeado_no_pool_e_fan_in(dag):
    calcular = dag.get_task("analise.calcular_metricas")

    assert calcular.is_mapped
    assert calcular.pool == "ecommerce_pool"
    assert calcular.downstream_task_ids == {"analise.consolidar_metricas"}
    assert dag.get_task("analise.consolidar_metricas").downstream_task_ids == {
        "persistencia.salvar_snapshot",
        "persistencia.salvar_historico",
    }


def test_busca_de_produtos_resiliente(dag):
    buscar = dag.get_task("ingestao.buscar_produtos")

    assert buscar.retries >= 3
    assert buscar.retry_exponential_backoff
    assert buscar.on_failure_callback
    assert buscar.on_retry_callback
    assert buscar.on_success_callback
