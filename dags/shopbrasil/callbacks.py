"""Callbacks de ciclo de vida das tasks e simulação de alertas."""

import logging

log = logging.getLogger(__name__)


def enviar_alerta(titulo: str, detalhes: str) -> None:
    """Simula o envio de um alerta para o time (Slack, e-mail, PagerDuty...).

    Em produção, este seria o ponto de integração com a ferramenta de
    alertas. Aqui o alerta só é registrado no log com destaque.
    """
    log.critical("[ALERTA ShopBrasil] %s | %s", titulo, detalhes)


def _descrever(context) -> str:
    ti = context["ti"]
    return (
        f"dag={ti.dag_id} task={ti.task_id} run={context.get('run_id')} "
        f"tentativa={ti.try_number}"
    )


def ao_concluir_task(context) -> None:
    """on_success_callback: registra que a task terminou bem."""
    log.info("Task concluída com sucesso: %s", _descrever(context))


def ao_retentar_task(context) -> None:
    """on_retry_callback: registra o motivo de cada nova tentativa."""
    log.warning(
        "Task falhou e será executada novamente: %s | motivo: %s",
        _descrever(context),
        context.get("exception"),
    )


def ao_falhar_task(context) -> None:
    """on_failure_callback: as tentativas acabaram, avisa o time."""
    descricao = _descrever(context)
    log.error("Task falhou após esgotar as tentativas: %s", descricao)
    enviar_alerta(
        "Falha na coleta de produtos",
        f"{descricao} | erro: {context.get('exception')}",
    )


def alertar_falha_dag(context) -> None:
    """on_failure_callback do DAG: qualquer falha na run gera um alerta.

    Garante que o time de pricing não fique sem dados de manhã sem que
    ninguém saiba. No Airflow 3 este callback roda no dag-processor.
    """
    dag_run = context.get("dag_run")
    dag_id = getattr(dag_run, "dag_id", None) or getattr(context.get("dag"), "dag_id", "?")
    run_id = getattr(dag_run, "run_id", None) or context.get("run_id", "?")
    enviar_alerta(
        "Pipeline de preços falhou",
        f"dag={dag_id} run={run_id} motivo={context.get('reason', 'task falhou')}",
    )
