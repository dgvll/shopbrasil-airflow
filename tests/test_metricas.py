import pytest

from shopbrasil import metricas


def produto(categoria, preco):
    return {"id": 1, "title": "x", "price": preco, "category": categoria}


def test_agrupa_precos_por_categoria_em_ordem_alfabetica():
    produtos = [produto("b", 10), produto("a", 5), produto("b", 20)]

    assert metricas.agrupar_por_categoria(produtos) == [
        {"categoria": "a", "precos": [5.0]},
        {"categoria": "b", "precos": [10.0, 20.0]},
    ]


def test_nova_categoria_gera_novo_grupo_sem_mudar_codigo():
    produtos = [produto("a", 1), produto("nova", 2)]

    categorias = [g["categoria"] for g in metricas.agrupar_por_categoria(produtos)]

    assert categorias == ["a", "nova"]


def test_calcula_medio_minimo_maximo_e_quantidade():
    resultado = metricas.calcular_metricas(
        "electronics", [64.0, 109.0, 109.0, 114.0, 599.0, 999.99]
    )

    assert resultado == {
        "categoria": "electronics",
        "qtd_produtos": 6,
        "preco_medio": 332.5,
        "preco_minimo": 64.0,
        "preco_maximo": 999.99,
    }


def test_arredonda_preco_medio_para_centavos_meio_para_cima():
    assert metricas.calcular_metricas("x", [0.01, 0.02])["preco_medio"] == 0.02


def test_categoria_sem_precos_falha():
    with pytest.raises(ValueError):
        metricas.calcular_metricas("vazia", [])


def test_consolidar_ordena_por_categoria():
    linhas = metricas.consolidar([{"categoria": "b"}, {"categoria": "a"}])

    assert [linha["categoria"] for linha in linhas] == ["a", "b"]


def test_consolidar_rejeita_categoria_duplicada():
    with pytest.raises(ValueError):
        metricas.consolidar([{"categoria": "a"}, {"categoria": "a"}])
