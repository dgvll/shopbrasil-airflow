# ShopBrasil — Pipeline de preços por categoria

Pipeline no **Apache Airflow 3.4** que substitui o script Python agendado via cron que alimentava o painel de preços da ShopBrasil.

Todo dia às **06:00 (horário de Brasília)** o DAG `shopbrasil_precos_categoria` coleta os produtos da API de catálogo ([FakeStore API](https://fakestoreapi.com/docs)), calcula **preço médio, mínimo, máximo e quantidade de produtos por categoria** e grava o resultado na base analítica em **PostgreSQL**.

O enunciado da atividade está em [docs/atividade-01-airflow.md](docs/atividade-01-airflow.md).

---

## Arquitetura

```
┌─ ingestao ──────────────────────────────────────────────────────────────────────────┐
│ preparar_tabelas → buscar_produtos → validar_produtos → agrupar_por_categoria       │  linear
└─────────────────────────────────────────────────────────────────────────┬───────────┘
┌─ analise ───────────────────────────────────────────────────────────────▼───────────┐
│            ┌→ calcular_metricas [electronics]      ─┐                               │
│  grupos ───┼→ calcular_metricas [jewelery]          ├→ consolidar_metricas          │  fan-out → fan-in
│            ├→ calcular_metricas [men's clothing]    │                               │
│            └→ calcular_metricas [women's clothing] ─┘   (pool ecommerce_pool, 2)    │
└─────────────────────────────────────────────────────────────────────────┬───────────┘
┌─ persistencia ──────────────────────────────────────────────────────────▼───────────┐
│                 salvar_snapshot (idempotente)   +   salvar_historico (append)       │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

| Serviço (docker compose) | Função |
|---|---|
| `postgres` | Banco de metadados do Airflow |
| `postgres-dw` | Base analítica `shopbrasil` (exposta em `localhost:5433`) |
| `airflow-init` | Migra o banco, cria o usuário da UI, a Connection `postgres_dw` e o Pool `ecommerce_pool` |
| `airflow-apiserver` | UI e API (http://localhost:8080) |
| `airflow-scheduler` | Agenda e executa as tasks (LocalExecutor) |
| `airflow-dag-processor` | Lê e serializa os DAGs |

O **LocalExecutor** foi escolhido no lugar do CeleryExecutor para economizar memória: não há Redis nem workers separados.

---

## Como cada requisito foi atendido

### Obrigatórios

| Requisito | Implementação |
|---|---|
| Fonte de dados | `GET https://fakestoreapi.com/products` em [`api.py`](dags/shopbrasil/api.py) |
| TaskFlow API | `@dag`, `@task` e `@task_group` de `airflow.sdk`; dependências saem da chamada das funções |
| XComs via `return` | Só os campos usados (`id`, `title`, `price`, `category`) trafegam entre as tasks |
| Topologias linear, fan-out e fan-in | `ingestao` (linear), `calcular_metricas.expand(...)` (fan-out) e `consolidar_metricas` (fan-in) |
| Agendamento | `schedule="0 6 * * *"`, `start_date=pendulum.datetime(2026, 11, 1, tz="America/Sao_Paulo")`, `catchup=False` |
| Retry com exponential backoff | `buscar_produtos`: `retries=4`, `retry_delay=20s`, `retry_exponential_backoff=True`, `max_retry_delay=5min` |
| try/except + raise | Erros de rede, HTTP, JSON inválido ou lista vazia viram `ErroApiCatalogo`, que é relançado para acionar o retry |
| Callbacks de ciclo de vida | `on_success_callback`, `on_retry_callback` e `on_failure_callback` em `buscar_produtos` ([`callbacks.py`](dags/shopbrasil/callbacks.py)) |
| Dynamic Task Mapping | Uma task `calcular_metricas` por categoria; nova categoria na API = nova task, sem mudar código |
| Pool | `ecommerce_pool` com 2 slots limita as tasks mapeadas |
| TaskGroups | `ingestao`, `analise` e `persistencia` |
| PostgresHook + Connection | `PostgresHook(postgres_conn_id="postgres_dw")` em [`banco.py`](dags/shopbrasil/banco.py) |
| Gravação idempotente | Chave primária `(data_referencia, categoria)` + `INSERT ... ON CONFLICT DO UPDATE` |

### Opcionais

| Requisito | Implementação |
|---|---|
| Operador customizado | `ValidarProdutosOperator(BaseOperator)` em [`operators.py`](dags/shopbrasil/operators.py): valida o schema, descarta produtos inválidos e falha se a proporção passar da tolerância |
| Tabela de histórico | `precos_categoria_historico` em modo append, com `executado_em` |
| SLA / alerta | O Airflow 3 removeu o SLA; o DAG usa `on_failure_callback=alertar_falha_dag`, que simula o envio de um alerta (log `[ALERTA ShopBrasil]`) |

---

## Estrutura do projeto

```
├── Dockerfile                  imagem apache/airflow:3.4.0 + requirements.txt
├── docker-compose.yaml         ambiente completo (Airflow + 2 bancos)
├── requirements.txt
├── .env.example                UID do host e usuário da UI
├── dags/
│   ├── shopbrasil_precos.py    definição do DAG (fina, só orquestra)
│   ├── .airflowignore          o pacote abaixo é importado, não parseado como DAG
│   └── shopbrasil/
│       ├── config.py           constantes (URL da API, Connection, Pool, timezone)
│       ├── api.py              cliente da API com tratamento de erros
│       ├── metricas.py         regras de negócio (funções puras)
│       ├── banco.py            DDL, snapshot idempotente e histórico
│       ├── callbacks.py        callbacks de ciclo de vida e alerta
│       └── operators.py        ValidarProdutosOperator
├── tests/                      testes de regras, do operador e de integridade do DAG
└── docs/atividade-01-airflow.md
```

---

## Como executar

Pré-requisitos: Docker com Docker Compose v2 e cerca de 4 GB de RAM livres.

```bash
cp .env.example .env          # no Linux, ajuste AIRFLOW_UID com o resultado de `id -u`
docker compose up -d --build
docker compose ps             # aguarde todos os serviços ficarem "healthy"
```

1. Acesse http://localhost:8080 (usuário `airflow`, senha `airflow`).
2. Ative o DAG `shopbrasil_precos_categoria` (ele é criado pausado).
3. Clique em **Trigger** para rodar agora, ou aguarde a execução das 06:00.

Pela linha de comando:

```bash
docker compose run --rm airflow-cli airflow dags unpause shopbrasil_precos_categoria
docker compose run --rm airflow-cli airflow dags trigger shopbrasil_precos_categoria
```

### Simular uma falha da API

Dispare o DAG com a configuração `{"simular_falha": true}` (na UI, em **Trigger** → *Run configuration*, ou pela CLI):

```bash
docker compose run --rm airflow-cli airflow dags trigger shopbrasil_precos_categoria -c '{"simular_falha": true}'
```

A task `buscar_produtos` falha, o `on_retry_callback` registra cada nova tentativa (com intervalo crescente) e, quando as tentativas acabam, o `on_failure_callback` da task e o do DAG disparam o alerta simulado.

### Consultar o resultado

```bash
docker compose exec postgres-dw psql -U dw -d shopbrasil
```

```sql
-- Panorama do dia (uma linha por categoria e data, sem duplicatas)
SELECT * FROM precos_categoria ORDER BY data_referencia DESC, categoria;

-- Evolução dos preços (uma linha por categoria a cada execução)
SELECT executado_em, categoria, preco_medio
FROM precos_categoria_historico
ORDER BY categoria, executado_em;
```

### Rodar os testes

```bash
docker compose run --rm airflow-cli python -m pytest
```

### Derrubar o ambiente

```bash
docker compose down        # mantém os dados
docker compose down -v     # apaga também os volumes dos bancos
```

---

## Decisões de projeto

- **Idempotência:** o snapshot usa `data_referencia` (a data da run no horário de Brasília) como parte da chave. Reprocessar a mesma run, ou rodar de novo no mesmo dia, atualiza as linhas em vez de duplicar. O histórico é append de propósito: cada execução vira um ponto na série de preços.
- **Data de referência:** runs agendadas usam a `logical_date`; runs manuais disparadas pela API do Airflow 3 podem não ter `logical_date`, então o pipeline usa o `run_after`.
- **XCom pequeno:** a ingestão devolve só os quatro campos usados no cálculo, e cada task mapeada recebe apenas os preços da sua categoria.
- **Escala com novas categorias:** o agrupamento gera a lista de categorias em tempo de execução, e o Dynamic Task Mapping cria uma task para cada uma. O pool limita a concorrência para não sobrecarregar a base.
- **Organização do código:** o arquivo do DAG só orquestra. As regras de negócio ficam em funções puras, fáceis de testar sem Airflow, e o acesso a API e banco fica isolado em módulos próprios.
