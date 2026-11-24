"""Cliente da API de catálogo (FakeStore API)."""

import logging

import requests

from shopbrasil.config import API_TIMEOUT_SEGUNDOS, API_URL

log = logging.getLogger(__name__)

# Só os campos usados no cálculo trafegam entre as tasks (XCom pequeno).
CAMPOS_PRODUTO = ("id", "title", "price", "category")


class ErroApiCatalogo(Exception):
    """Falha ao obter os produtos da API de catálogo."""


def buscar_produtos_api(url: str = API_URL, timeout: int = API_TIMEOUT_SEGUNDOS) -> list[dict]:
    """Busca os produtos na API e devolve apenas os campos necessários.

    Qualquer problema (rede, HTTP != 2xx, JSON inválido ou lista vazia) vira
    ErroApiCatalogo, para que a task falhe e o Airflow faça o retry.
    """
    try:
        resposta = requests.get(
            url,
            timeout=timeout,
            headers={"Accept": "application/json", "User-Agent": "shopbrasil-airflow/1.0"},
        )
        resposta.raise_for_status()
        produtos = resposta.json()
    except requests.RequestException as erro:
        log.error("Falha de comunicação com a API de catálogo: %s", erro)
        raise ErroApiCatalogo(f"Erro ao chamar {url}: {erro}") from erro
    except ValueError as erro:
        log.error("A API de catálogo devolveu um JSON inválido: %s", erro)
        raise ErroApiCatalogo(f"Resposta inválida de {url}") from erro

    if not isinstance(produtos, list) or not produtos:
        raise ErroApiCatalogo(f"A API {url} não devolveu nenhum produto")

    log.info("%d produtos recebidos da API de catálogo", len(produtos))
    return [{campo: produto.get(campo) for campo in CAMPOS_PRODUTO} for produto in produtos]
