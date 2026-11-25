"""Operadores customizados do pipeline."""

from numbers import Real

from airflow.exceptions import AirflowException
from airflow.sdk import BaseOperator


class ValidarProdutosOperator(BaseOperator):
    """Valida o schema dos produtos antes do processamento.

    Cada produto precisa de id inteiro, título e categoria não vazios e preço
    numérico maior ou igual a zero. Produtos inválidos são descartados e
    registrados no log; a task falha se não sobrar nenhum produto válido ou se
    a proporção de inválidos passar da tolerância (sinal de que a API mudou).

    :param produtos: lista de produtos (normalmente o XCom da task anterior).
    :param tolerancia_invalidos: fração máxima de produtos inválidos aceita.
    """

    template_fields = ("produtos",)
    ui_color = "#fdf2d0"

    def __init__(self, *, produtos, tolerancia_invalidos: float = 0.2, **kwargs):
        super().__init__(**kwargs)
        self.produtos = produtos
        self.tolerancia_invalidos = tolerancia_invalidos

    @staticmethod
    def problemas(produto) -> list[str]:
        """Lista os problemas de schema de um produto (vazia se estiver ok)."""
        if not isinstance(produto, dict):
            return ["não é um objeto"]

        erros = []
        produto_id = produto.get("id")
        if not isinstance(produto_id, int) or isinstance(produto_id, bool):
            erros.append("id ausente ou não inteiro")

        for campo in ("title", "category"):
            valor = produto.get(campo)
            if not isinstance(valor, str) or not valor.strip():
                erros.append(f"{campo} ausente ou vazio")

        preco = produto.get("price")
        if not isinstance(preco, Real) or isinstance(preco, bool):
            erros.append("price ausente ou não numérico")
        elif preco < 0:
            erros.append("price negativo")

        return erros

    def execute(self, context) -> list[dict]:
        produtos = list(self.produtos or [])
        if not produtos:
            raise AirflowException("Nenhum produto recebido para validar")

        validos = []
        invalidos = 0
        for produto in produtos:
            erros = self.problemas(produto)
            if erros:
                invalidos += 1
                self.log.warning("Produto descartado %r: %s", produto, "; ".join(erros))
            else:
                validos.append(produto)

        proporcao = invalidos / len(produtos)
        self.log.info(
            "Validação concluída: %d válidos, %d inválidos (%.0f%%)",
            len(validos),
            invalidos,
            proporcao * 100,
        )
        if not validos:
            raise AirflowException("Nenhum produto passou na validação de schema")
        if proporcao > self.tolerancia_invalidos:
            raise AirflowException(
                f"{proporcao:.0%} dos produtos são inválidos "
                f"(tolerância {self.tolerancia_invalidos:.0%}); a API pode ter mudado"
            )
        return validos
