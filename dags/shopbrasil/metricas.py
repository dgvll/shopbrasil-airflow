"""Regras de negócio do panorama de preços (funções puras, fáceis de testar)."""

from collections import defaultdict
from decimal import ROUND_HALF_UP, Decimal


def _arredondar(valor: float) -> float:
    """Arredonda para centavos (meio para cima), como no painel de preços."""
    return float(Decimal(str(valor)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def agrupar_por_categoria(produtos: list[dict]) -> list[dict]:
    """Agrupa os preços por categoria, uma entrada por categoria.

    O resultado alimenta o Dynamic Task Mapping: cada item vira uma task.
    Novas categorias na API geram novas tasks sem mudar o código.
    """
    precos_por_categoria: dict[str, list[float]] = defaultdict(list)
    for produto in produtos:
        precos_por_categoria[produto["category"]].append(float(produto["price"]))

    return [
        {"categoria": categoria, "precos": precos_por_categoria[categoria]}
        for categoria in sorted(precos_por_categoria)
    ]


def calcular_metricas(categoria: str, precos: list[float]) -> dict:
    """Preço médio, mínimo, máximo e quantidade de produtos de uma categoria."""
    if not precos:
        raise ValueError(f"A categoria '{categoria}' não tem preços para calcular")

    return {
        "categoria": categoria,
        "qtd_produtos": len(precos),
        "preco_medio": _arredondar(sum(precos) / len(precos)),
        "preco_minimo": _arredondar(min(precos)),
        "preco_maximo": _arredondar(max(precos)),
    }


def consolidar(resultados: list[dict]) -> list[dict]:
    """Junta as métricas de todas as categorias em uma lista ordenada.

    Falha se a mesma categoria aparecer duas vezes, o que indicaria um erro
    no agrupamento e geraria dados inconsistentes no painel.
    """
    linhas = sorted(resultados, key=lambda linha: linha["categoria"])
    categorias = [linha["categoria"] for linha in linhas]
    if len(categorias) != len(set(categorias)):
        raise ValueError(f"Categorias duplicadas na consolidação: {categorias}")
    return linhas
