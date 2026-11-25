"""Acesso à base analítica (PostgreSQL) via PostgresHook."""

from datetime import date, datetime
from zoneinfo import ZoneInfo

from airflow.providers.postgres.hooks.postgres import PostgresHook

from shopbrasil.config import POSTGRES_CONN_ID, TIMEZONE

COLUNAS_METRICAS = ("categoria", "qtd_produtos", "preco_medio", "preco_minimo", "preco_maximo")

DDL = """
CREATE TABLE IF NOT EXISTS precos_categoria (
    data_referencia DATE           NOT NULL,
    categoria       TEXT           NOT NULL,
    qtd_produtos    INTEGER        NOT NULL,
    preco_medio     NUMERIC(10, 2) NOT NULL,
    preco_minimo    NUMERIC(10, 2) NOT NULL,
    preco_maximo    NUMERIC(10, 2) NOT NULL,
    run_id          TEXT           NOT NULL,
    atualizado_em   TIMESTAMPTZ    NOT NULL DEFAULT now(),
    PRIMARY KEY (data_referencia, categoria)
);

CREATE TABLE IF NOT EXISTS precos_categoria_historico (
    id              BIGSERIAL      PRIMARY KEY,
    executado_em    TIMESTAMPTZ    NOT NULL,
    data_referencia DATE           NOT NULL,
    categoria       TEXT           NOT NULL,
    qtd_produtos    INTEGER        NOT NULL,
    preco_medio     NUMERIC(10, 2) NOT NULL,
    preco_minimo    NUMERIC(10, 2) NOT NULL,
    preco_maximo    NUMERIC(10, 2) NOT NULL,
    run_id          TEXT           NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_historico_categoria_data
    ON precos_categoria_historico (categoria, data_referencia);
"""


def _hook() -> PostgresHook:
    return PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)


def criar_tabelas() -> None:
    """Cria as tabelas, se ainda não existirem (seguro para rodar sempre)."""
    _hook().run(DDL)


def salvar_snapshot(data_referencia: date, linhas: list[dict], run_id: str) -> int:
    """Grava o panorama do dia de forma idempotente.

    A chave primária (data_referencia, categoria) com INSERT ... ON CONFLICT
    DO UPDATE garante que reprocessar a mesma data atualiza as linhas em vez
    de duplicá-las. Todas as linhas entram na mesma transação.
    """
    agora = datetime.now(ZoneInfo(TIMEZONE))
    registros = [
        (data_referencia, *(linha[coluna] for coluna in COLUNAS_METRICAS), run_id, agora)
        for linha in linhas
    ]
    _hook().insert_rows(
        table="precos_categoria",
        rows=registros,
        target_fields=["data_referencia", *COLUNAS_METRICAS, "run_id", "atualizado_em"],
        replace=True,
        replace_index=["data_referencia", "categoria"],
    )
    return len(registros)


def inserir_historico(data_referencia: date, linhas: list[dict], run_id: str) -> int:
    """Acrescenta as métricas ao histórico (modo append), com a data da execução.

    Diferente do snapshot, aqui toda execução gera novas linhas: é isso que
    permite acompanhar a evolução dos preços ao longo do tempo.
    """
    executado_em = datetime.now(ZoneInfo(TIMEZONE))
    registros = [
        (executado_em, data_referencia, *(linha[coluna] for coluna in COLUNAS_METRICAS), run_id)
        for linha in linhas
    ]
    _hook().insert_rows(
        table="precos_categoria_historico",
        rows=registros,
        target_fields=["executado_em", "data_referencia", *COLUNAS_METRICAS, "run_id"],
    )
    return len(registros)
