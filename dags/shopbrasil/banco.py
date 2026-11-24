"""Acesso à base analítica (PostgreSQL) via PostgresHook."""

from airflow.providers.postgres.hooks.postgres import PostgresHook

from shopbrasil.config import POSTGRES_CONN_ID

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
"""


def _hook() -> PostgresHook:
    return PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)


def criar_tabelas() -> None:
    """Cria as tabelas, se ainda não existirem (seguro para rodar sempre)."""
    _hook().run(DDL)
