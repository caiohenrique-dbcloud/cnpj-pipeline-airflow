# Pipeline CNPJ — Matriz vs. Filial em São Paulo (com Apache Airflow)

Pipeline de dados que responde à pergunta de negócio do time de Inteligência
de Mercado:

> **Quantas filiais vs. matrizes com Situação Cadastral 'Ativa' existem na
> cidade de São Paulo?**

Fonte: [Dados Abertos do CNPJ — Receita Federal](https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj).

Este repositório é a evolução do [pipeline base](https://github.com/caiohenrique-dbcloud/cnpj-pipeline),
adicionando orquestração com **Apache Airflow**.

---

## Resultado validado

Executando a pipeline via Airflow, com dados reais da Receita Federal
(amostra de uma partição do arquivo de dezembro/2025):

| Tipo | Quantidade |
|---|---|
| Matriz | 984.186 |
| Filial | 33.035 |
| **Total** | **1.017.221** |

Todas as três tarefas da DAG (Bronze → Silver → Gold) concluídas com sucesso.

---

## Arquitetura

```
Receita Federal (.zip)          BRONZE                SILVER                    GOLD
   ┌───────────┐         ┌──────────────────┐   ┌────────────────────┐   ┌──────────────────┐
   │ Estabelec.│──ingest→│ .zip brutos, sem  │──→│ Parquet filtrado:   │──→│ Contagem Matriz/  │
   │ Municipios│         │ transformação     │  process   Ativa + SP  │   │ Filial + gráfico  │
   └───────────┘         └──────────────────┘   └────────────────────┘   └──────────────────┘
                          (data lake / MinIO)     (data lake / MinIO)      (data lake / MinIO)
```

```
                    ┌─────────────────────────────────────┐
                    │         Docker Compose                │
  ┌──────────┐      │  ┌───────────┐    ┌────────────────┐ │
  │  Browser  │─────┼─▶│  Webserver │───▶│   Scheduler    │ │
  └──────────┘      │  └─────┬─────┘    └────────┬───────┘ │
                    │        ▼                    ▼         │
                    │  ┌───────────┐      ┌───────────────┐ │
                    │  │  Postgres  │      │     MinIO      │ │
                    │  │ (metadados │      │ (Bronze/Silver/│ │
                    │  │ do Airflow)│      │     Gold)      │ │
                    │  └───────────┘      └───────────────┘ │
                    └─────────────────────────────────────┘
```

Segue a **Arquitetura Medallion**:

| Camada | Conteúdo | Formato | Por quê |
|---|---|---|---|
| **Bronze** | Cópia fiel do `.zip` baixado da RFB | ZIP (raw) | Preserva o dado original. |
| **Silver** | Estabelecimentos de SP, Ativos, já cruzados com Municípios | Parquet | Formato colunar, comprimido, com tipos definidos. |
| **Gold** | Tabela agregada Matriz x Filial + gráfico | CSV + PNG | Pronto para consumo direto por negócio/BI. |

### Por que Airflow

Transformar as 3 camadas em tarefas orquestradas (em vez de um script
sequencial único) traz vantagens reais de operação:

- Retry automático por tarefa, isolado — uma falha na Silver não obriga
  reprocessar a Bronze.
- Agendamento nativo (a DAG está configurada para rodar mensalmente).
- Rastreabilidade completa via metadados: cada execução, cada tentativa e
  sua duração ficam registradas e são consultáveis via SQL.
- Parametrização pela própria interface (ano, mês, modo de teste), sem
  precisar alterar código para rodar um período diferente.

### Por que DuckDB em vez de Spark

Os arquivos de `Estabelecimentos` somam vários GB descompactados. Em vez de
um cluster Spark, o **DuckDB** processa os CSVs em streaming (out-of-core)
com SQL puro — trade-off consciente de custo-benefício para este volume de
dados rodando em uma única máquina.

### Por que MinIO

Simula localmente um Data Lake S3-compatível: o mesmo código roda apontando
para MinIO local ou para um S3 real em produção, só trocando variáveis de
ambiente (ver `src/storage.py`).

---

## Estrutura de pastas

```
cnpj-pipeline-airflow/
├── docker-compose.yml       # MinIO + Postgres + Airflow (webserver/scheduler)
├── Dockerfile                # imagem do serviço pipeline standalone
├── Dockerfile.airflow         # imagem do Airflow com as dependências do projeto
├── requirements.txt
├── dags/
│   └── cnpj_pipeline_dag.py  # DAG: bronze -> silver -> gold
├── src/
│   ├── config.py              # configurações e regras de negócio
│   ├── storage.py             # abstração MinIO <-> S3 <-> disco local
│   ├── ingest.py               # camada Bronze (download + retomada + fallback)
│   ├── process.py             # camada Silver
│   ├── aggregate.py           # camada Gold
│   └── pipeline.py            # CLI standalone (sem Airflow)
├── tests/
│   └── test_pipeline.py       # testes unitários (lógica pura, sem rede)
└── data/                       # usado apenas quando USE_MINIO=false
```

---

## Como rodar

Pré-requisitos: Docker e Docker Compose.

```bash
git clone <url-do-seu-repositorio>
cd cnpj-pipeline-airflow

docker compose up airflow-init      # roda uma vez: cria o banco e o usuário admin
docker compose up airflow-webserver airflow-scheduler minio -d
```

Acesse `http://localhost:8080` — login `admin` / senha `admin`.

Ative a DAG `cnpj_pipeline_matriz_filial_sp` e clique em **"Trigger DAG
w/ config"** para rodar com parâmetros customizados:

```json
{"year": 2025, "month": 12, "sample": 1, "mock": false}
```

| Parâmetro | Descrição |
|---|---|
| `year` / `month` | Período de referência (com fallback automático para o mês anterior se ainda não publicado) |
| `sample` | Limita a quantidade de partições de Estabelecimentos baixadas (útil para validação rápida) |
| `mock` | Se `true`, gera dados sintéticos localmente, sem acessar a internet (ver seção abaixo) |

### Testando sem depender de download (modo mock)

O projeto inclui um modo de teste com **dados sintéticos**, isolado por
namespace dos dados reais (prefixo `MOCK-` no armazenamento — nunca ocupa
o mesmo endereço que dados de produção usariam). Isso permite validar toda
a orquestração em segundos, sem depender de baixar arquivos de ~2GB:

```json
{"year": 2025, "month": 12, "mock": true}
```

### Executando sem o Airflow (script direto)

Também é possível rodar a pipeline como script standalone, sem orquestração:

```bash
docker compose run --rm pipeline --year 2025 --month 12 --sample 1
```

### Rodando os testes

```bash
docker compose run --rm --entrypoint pytest pipeline tests/ -v
```

---

## Desafios técnicos resolvidos

**Resiliência a instabilidade da fonte de dados oficial.** O domínio
oficial da Receita Federal apresenta indisponibilidade frequente. A
ingestão tenta, em ordem, dois domínios oficiais e, na falha de ambos, um
mirror mantido pela Casa dos Dados — com estrutura de pastas diferente,
tratada à parte na resolução do período.

**Retomada de download (resume).** Arquivos de ~2GB em conexões instáveis
frequentemente têm a conexão interrompida no meio do streaming. A
implementação grava em um arquivo `.partial` e, ao reconectar, retoma a
partir do último byte confirmado (cabeçalho HTTP `Range`), em vez de
reiniciar o download do zero — reduzindo drasticamente o tempo total em
cenários de rede instável.

**Isolamento entre dados de teste e dados de produção.** Uma versão
inicial do modo `mock` gravava os dados sintéticos no mesmo endereço de
armazenamento (`ano-mês`) que os dados reais usariam. Isso fazia com que
uma execução real subsequente, ao checar idempotência, encontrasse os
dados fictícios e pulasse o download real — contaminando o resultado
silenciosamente. Corrigido isolando o namespace de armazenamento por modo
de execução (`MOCK-ano-mês` vs. `ano-mês`).

**Resolução de DNS intermitente em ambiente containerizado.** Em cenários
de instabilidade de rede prolongada, a resolução de nomes de domínio
dentro dos containers pode degradar mesmo com conectividade normal na
máquina host. Mitigado fixando servidores DNS públicos nos serviços que
acessam a internet.

**Timeout de inicialização do servidor web em ambientes com recurso
limitado.** O tempo padrão de inicialização dos processos do Airflow
Webserver pode não ser suficiente em ambientes com CPU/memória
compartilhados. Ajustado via configuração de timeout e redução do número
de workers.

**Nomeação inconsistente de colunas ao ler CSV sem cabeçalho.** O motor de
leitura infere nomes de coluna (`columnN`) cuja formatação varia conforme
o número total de colunas do arquivo. Resolvido especificando os nomes de
coluna explicitamente na leitura, eliminando a dependência desse
comportamento implícito.

**Encoding incompatível com a versão da biblioteca de processamento.**
Os CSVs da RFB são publicados em `latin-1`; a leitura via SQL nessa versão
específica do motor de processamento não aceitava esse parâmetro
diretamente. Resolvido convertendo os arquivos para `utf-8` em streaming
antes da leitura.

---

## Decisões e limitações conhecidas

- O pipeline baixa apenas `Estabelecimentos*.zip` e `Municipios.zip` — os
  únicos necessários para responder à pergunta de negócio.
- O resultado documentado acima refere-se a uma amostra (`sample: 1`, uma
  das ~10 partições do arquivo); o mês completo requer mais tempo de
  download, mas usa exatamente a mesma lógica de negócio.
- Postgres neste projeto armazena **apenas metadados de orquestração do
  Airflow** (histórico de execuções) — não contém dados de negócio, que
  residem inteiramente no MinIO/S3.
- Próximos passos: migração de MinIO para AWS S3, carga em Snowflake via
  `COPY INTO`, transformações com dbt, e dashboard em Power BI.
