"""Constantes do pipeline, centralizadas para facilitar a manutenção."""

import os

TIMEZONE = "America/Sao_Paulo"

# API de catálogo (FakeStore API). Pode ser sobrescrita por variável de ambiente.
API_URL = os.getenv("SHOPBRASIL_API_URL", "https://fakestoreapi.com/products")
API_TIMEOUT_SEGUNDOS = 15

# Connection e Pool criados pelo serviço airflow-init (docker-compose.yaml).
POSTGRES_CONN_ID = "postgres_dw"
POOL_CATEGORIAS = "ecommerce_pool"
