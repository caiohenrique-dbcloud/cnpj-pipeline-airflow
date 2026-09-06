# Pipeline CNPJ — Matriz vs. Filial em São Paulo

Pipeline de dados que responde à pergunta de negócio do time de Inteligência
de Mercado:

> **Quantas filiais vs. matrizes com Situação Cadastral 'Ativa' existem na
> cidade de São Paulo?**

Fonte: [Dados Abertos do CNPJ — Receita Federal](https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj).

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

Segue a **Arquitetura Medallion**:

| Camada | Conteúdo | Formato | Por quê |
|---|---|---|---|
| **Bronze** | Cópia fiel do `.zip` baixado da RFB | ZIP (raw) | Preserva o dado original — se a lógica de negócio mudar amanhã, reprocessamos sem baixar de novo. |
| **Silver** | Estabelecimentos de SP, Ativos, já cruzados com Municípios | Parquet | Formato colunar, comprimido, rápido de ler nas próximas etapas. Já limpo e com tipos definidos. |
| **Gold** | Tabela agregada Matriz x Filial + gráfico | CSV + PNG | Pronto para consumo direto por negócio/BI, sem reprocessamento. |

### Por que DuckDB em vez de Spark?

Os arquivos de `Estabelecimentos` somam vários GB descompactados. Carregar
tudo em memória com pandas não é viável numa máquina comum. Em vez de subir
um cluster Spark (que exige mais infraestrutura), usei o **DuckDB**, que lê
os CSVs em streaming (out-of-core) com SQL puro e escala muito bem para um
volume desse porte numa única máquina — um trade-off consciente para um MVP.

> Em um cenário de produção, com histórico de múltiplos meses/UFs sendo
> processado continuamente, o próximo passo natural seria migrar essa mesma
> lógica SQL para **PySpark** (para paralelismo real em cluster) orquestrado
> via **Airflow** (agendamento, retries, monitoramento, backfill). A
> estrutura em camadas e a separação de responsabilidades (`ingest.py` /
> `process.py` / `aggregate.py`) já foi pensada para facilitar essa migração
> — cada função viraria uma Task de uma DAG.

### Por que MinIO?

Simula localmente um Data Lake S3-compatível, o que deixa o projeto
portável: o mesmo código roda apontando para MinIO local ou para um S3 real
em produção, só trocando variáveis de ambiente — sem alterar uma linha de
código (ver `src/storage.py`).

### Como o cruzamento de dados foi feito

O arquivo de Estabelecimentos traz apenas o **código do município** (não o
nome). Para filtrar "São Paulo" corretamente, foi necessário:

1. Ler `Municipios.zip` (tabela de referência código → nome).
2. Fazer `INNER JOIN` entre Estabelecimentos e Municípios pelo código.
3. Filtrar `nome_municipio = 'SAO PAULO'` (sem acento — assim os dados vêm
   na fonte) **e** `situacao_cadastral = '02'` (código de "Ativa" no layout
   oficial da RFB).
4. Segregar Matriz/Filial pela coluna `identificador_matriz_filial`
   (`1 = Matriz`, `2 = Filial`).

Todo esse cruzamento acontece em uma única query SQL no DuckDB
(`src/process.py`), evitando um join custoso em pandas.

### Idempotência

Antes de executar cada camada, o pipeline verifica se a saída daquele
período **já existe** no storage (`Storage.exists`). Se existir, a etapa é
pulada — reexecutar o pipeline para o mesmo mês não duplica nem corrompe
dados. Use `--force` para reprocessar mesmo assim (ex: se a lógica de
negócio mudou).

### Fallback de período (mês mais recente disponível)

Se o mês/ano pedido ainda não tiver sido publicado pela Receita, o pipeline
tenta automaticamente os meses anteriores (até 3, configurável) e usa o
primeiro que encontrar dados publicados — atendendo ao requisito "Dezembro
de 2025, ou o mês mais recente disponível".

---

## Estrutura de pastas

```
cnpj-pipeline/
├── docker-compose.yml     # orquestra MinIO + pipeline
├── Dockerfile
├── requirements.txt
├── .env.example
├── src/
│   ├── config.py           # configurações e regras de negócio centralizadas
│   ├── storage.py          # abstração MinIO <-> disco local
│   ├── ingest.py           # camada Bronze
│   ├── process.py          # camada Silver
│   ├── aggregate.py        # camada Gold
│   └── pipeline.py         # CLI / orquestrador
├── tests/
│   └── test_pipeline.py    # testes unitários (lógica pura, sem rede)
└── data/                   # usado apenas quando USE_MINIO=false
```

---

## Como rodar

### Opção A — Docker Compose (recomendado)

Pré-requisitos: Docker Desktop com integração WSL2 habilitada (Windows) ou
Docker + Docker Compose nativos (Mac/Linux).

```bash
git clone <url-do-seu-repositorio>
cd cnpj-pipeline

docker compose build
docker compose run --rm pipeline --year 2025 --month 12 --sample 1
```

**Por que `--sample 1` como comando recomendado:** os arquivos de
`Estabelecimentos` vêm divididos em ~10 partes de ~1-2GB cada. Baixar o mês
inteiro pode levar bastante tempo dependendo da internet e da estabilidade da
fonte (ver seção de desafios abaixo). `--sample 1` baixa só a primeira parte,
o que é suficiente para validar toda a pipeline (Bronze → Silver → Gold)
rapidamente. O resultado com `--sample` é uma **amostra parcial** dos CNPJs
do mês, não o número completo e exato de São Paulo.

Para o resultado **completo e correto** (respondendo à pergunta de negócio
com o dado real), rode sem essa flag — mas reserve mais tempo, pois envolve
baixar todas as partições:

```bash
docker compose run --rm pipeline --year 2025 --month 12
```

Isso vai:
1. Subir o MinIO (console web em `http://localhost:9001`, login `minioadmin`/`minioadmin`).
2. Rodar o pipeline completo (Bronze → Silver → Gold) para o período informado.

Graças à idempotência, rodar de novo (com ou sem `--sample`) não baixa de
novo o que já foi baixado — só completa o que falta.

### Opção B — Local, sem Docker (mais simples para desenvolvimento)

Pré-requisitos: Python 3.11+.

```bash
git clone <url-do-seu-repositorio>
cd cnpj-pipeline

python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate

pip install -r requirements.txt
cp .env.example .env        # USE_MINIO=false por padrão -> grava em ./data

python -m src.pipeline --year 2025 --month 12 --sample 1
```

Os resultados finais ficam em `data/gold/<ano>-<mes>/`:
- `matriz_vs_filial_sp.csv` — tabela agregada.
- `matriz_vs_filial_sp.png` — gráfico de barras.


### Rodando os testes

```bash
pytest -v
```

### Testando sem depender da internet (modo mock)

Se a fonte de dados estiver instável ou sua conexão não for confiável para
baixar arquivos de ~2GB, use o modo `mock` — ele gera dados sintéticos
localmente (12 matrizes + 7 filiais ativas em SP, mais alguns registros de
"ruído" para validar o filtro), sem acessar a internet nenhuma vez:

```bash
# Docker
docker compose run --rm pipeline --year 2025 --month 12 --mock

# Airflow: no Trigger DAG w/ config, marque o campo "mock" como true
```

Isso valida toda a orquestração (Bronze → Silver → Gold, Airflow, retries,
XCom) em segundos, útil para separar "meu pipeline funciona" de "minha
internet aguenta baixar 2GB agora" — são duas perguntas diferentes.

### Testando rapidamente, sem esperar o mês inteiro baixar

Os arquivos de `Estabelecimentos` vêm divididos em ~10 partes. Para validar
que o pipeline inteiro funciona (bronze → silver → gold) sem esperar todas
baixarem, use `--sample N` para baixar só as N primeiras partes:

```bash
# Docker
docker compose run --rm pipeline --year 2025 --month 12 --sample 1

# Local
python -m src.pipeline --year 2025 --month 12 --sample 1
```

⚠️ Com `--sample`, o resultado final é uma **amostra parcial** (só uma fração
dos CNPJs do mês), não a contagem real de São Paulo — serve apenas para
validar que a pipeline roda ponta a ponta sem erros. Para o resultado
correto e completo, rode sem essa flag.

---

## Orquestração com Airflow (evolução do MVP)

Além de rodar via `python -m src.pipeline` (script direto), o projeto agora
também pode ser orquestrado pelo **Apache Airflow**, transformando as 3
camadas em tarefas independentes de uma DAG (`dags/cnpj_pipeline_dag.py`).

**Por que isso é uma evolução real, não só "rodar o mesmo código diferente":**
- Se só a camada Silver falhar, o Airflow reprocessa **só ela** — não
  precisa baixar tudo de novo (a idempotência que já existia no projeto
  passa a ser aproveitada automaticamente pelo orquestrador).
- Retries automáticos configurados por tarefa (3 tentativas, 2 min de
  espera entre elas).
- Agendamento nativo (roda sozinho todo dia 20 do mês, sem precisar de
  ninguém disparar manualmente).
- Interface visual (`localhost:8080`) mostrando o progresso de cada etapa.

### Como subir o Airflow

```bash
docker compose up airflow-init      # roda uma vez, cria o banco e o usuário admin
docker compose up airflow-webserver airflow-scheduler minio -d
```

Acessa `http://localhost:8080` — login `admin` / senha `admin`.

Na interface, ativa a DAG `cnpj_pipeline_matriz_filial_sp` (ela vem pausada
por padrão) e clica em "Trigger DAG" para rodar manualmente. Para rodar com
parâmetros diferentes de ano/mês/amostra, usa "Trigger DAG w/ config":
```json
{"year": 2025, "month": 12, "sample": 1}
```

### O que muda internamente

As tarefas da DAG (`_task_bronze`, `_task_silver`, `_task_gold` em
`dags/cnpj_pipeline_dag.py`) chamam **as mesmas funções** de
`src/ingest.py`, `src/process.py` e `src/aggregate.py` — nenhuma lógica de
negócio foi duplicada. O Airflow só decide **quando e como** rodar cada
etapa; a lógica de filtrar SP/Ativa/Matriz-Filial continua isolada e
testável independentemente (`tests/test_pipeline.py` continua válido).

---

## Desafios enfrentados durante o desenvolvimento

Documentado de propósito — problemas de engenharia de dados do "mundo real"
que apareceram testando este projeto contra a fonte de dados de verdade, e
como cada um foi resolvido:

**1. Domínio oficial da RFB instável (`dadosabertos.rfb.gov.br`)**
Esse portal frequentemente fica lento ou fora do ar (comum em portais
governamentais de grande volume de acesso). Solução: `ingest.py` tenta, em
ordem, dois domínios oficiais e, se ambos falharem, um **mirror** mantido
pela Casa dos Dados (mesma fonte, replicada via CDN) — com estrutura de
pastas diferente (`YYYY-MM-DD/` em vez de `YYYY-MM/`), tratada à parte na
função `_list_mirror_folder_and_files`.

**2. Downloads grandes quebrando no meio (`ChunkedEncodingError` / `IncompleteRead`)**
Arquivos de ~1-2GB por partição, em conexão doméstica, ocasionalmente têm a
conexão derrubada no meio do streaming. Solução: retry automático com
backoff em `_download_file` (até 4 tentativas, descartando o arquivo
parcial antes de tentar de novo).

**3. Falha de resolução de DNS dentro do container (WSL2 + Docker)**
Após o notebook suspender/dormir com o container rodando, o DNS interno do
Docker (no ambiente WSL2) parou de resolver nomes de domínio
(`Name or service not known`), mesmo com internet normal no host. Solução:
DNS fixo (`8.8.8.8`, `1.1.1.1`) configurado no `docker-compose.yml` para o
serviço `pipeline`, mais o hábito de rodar `wsl --shutdown` (PowerShell)
para resetar o estado de rede quando isso ocorre.

**4. Parâmetro `encoding` incompatível com a versão do DuckDB usada**
`read_csv_auto(..., encoding='latin-1')` não é aceito nessa versão do
DuckDB (função não tem esse parâmetro nomeado). Solução: converter os CSVs
de `latin-1` para `utf-8` em streaming logo após a extração do zip
(`_transcode_latin1_to_utf8` em `process.py`), antes de qualquer leitura
via SQL.

**5. Nomeação automática de colunas sem cabeçalho, inconsistente entre arquivos**
Ao ler CSVs sem header, o DuckDB nomeia colunas como `columnN`, mas a
quantidade de dígitos/zeros à esquerda usada varia conforme o número total
de colunas do arquivo (`column00`.."column29" para 30 colunas, mas
`column0`/`column1` — sem zero à esquerda — para um arquivo de só 2
colunas como `Municipios`). Isso quebrou a query na primeira tentativa.
Solução definitiva (mais robusta que só ajustar o índice): usar o parâmetro
`names=[...]` do `read_csv_auto` para nomear as colunas explicitamente na
leitura, eliminando de vez a dependência desse comportamento interno.

---

## Decisões e limitações conhecidas (transparência)

- **Escopo dos arquivos baixados:** o pipeline baixa apenas
  `Estabelecimentos*.zip` e `Municipios.zip`, os únicos necessários para
  responder à pergunta de negócio. Os arquivos de `Empresas`, `Socios`,
  `Simples` etc. não são baixados, o que evita dezenas de GB de tráfego
  desnecessário para este MVP.
- **Volume de dados:** os arquivos de Estabelecimentos são grandes (vários
  GB por mês, em 10 partes). Em uma máquina com pouca memória/disco, a
  primeira execução pode demorar — isso é esperado e é justamente o
  problema que o DuckDB (streaming) resolve, em vez de carregar tudo em
  pandas.
- **Encoding:** os CSVs da RFB vêm em `latin-1` e separados por `;`,
  tratado explicitamente em `process.py`.
- **Próximos passos naturais** (fora do escopo deste MVP, mas documentados
  como evolução): orquestração via Airflow com agendamento mensal
  automático, migração do processamento para PySpark em cluster, alertas
  de qualidade de dados (ex: `great_expectations`), e particionamento
  histórico por mês na camada gold para permitir séries temporais.
