"""Regras de negócio do panorama de preços (funções puras, fáceis de testar)."""

from collections import defaultdict


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
