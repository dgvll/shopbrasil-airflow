import pytest
from airflow.exceptions import AirflowException

from shopbrasil.operators import ValidarProdutosOperator

VALIDO = {"id": 1, "title": "Mochila", "price": 109.95, "category": "men's clothing"}


def operador(produtos, **kwargs):
    return ValidarProdutosOperator(task_id="validar_produtos", produtos=produtos, **kwargs)


def test_produto_valido_nao_tem_problemas():
    assert ValidarProdutosOperator.problemas(VALIDO) == []


@pytest.mark.parametrize(
    ("alteracao", "problema"),
    [
        ({"id": None}, "id ausente ou não inteiro"),
        ({"id": True}, "id ausente ou não inteiro"),
        ({"title": "  "}, "title ausente ou vazio"),
        ({"category": None}, "category ausente ou vazio"),
        ({"price": "10.00"}, "price ausente ou não numérico"),
        ({"price": -1}, "price negativo"),
    ],
)
def test_detecta_problemas_de_schema(alteracao, problema):
    assert problema in ValidarProdutosOperator.problemas({**VALIDO, **alteracao})


def test_descarta_invalidos_dentro_da_tolerancia():
    produtos = [VALIDO] * 9 + [{**VALIDO, "price": None}]

    validos = operador(produtos, tolerancia_invalidos=0.2).execute(context={})

    assert len(validos) == 9


def test_falha_quando_invalidos_passam_da_tolerancia():
    produtos = [VALIDO, {**VALIDO, "price": None}]

    with pytest.raises(AirflowException, match="tolerância"):
        operador(produtos, tolerancia_invalidos=0.2).execute(context={})


def test_falha_sem_produtos():
    with pytest.raises(AirflowException):
        operador([]).execute(context={})
