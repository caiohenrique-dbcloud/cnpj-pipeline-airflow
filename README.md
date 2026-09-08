**Stack:** Python · Docker · Apache Airflow · AWS S3 · DuckDB
## O que este projeto adiciona em relação ao pipeline base

Este repositório evolui o [pipeline base](https://github.com/caiohenrique-dbcloud/cnpj-pipeline)
em duas dimensões que o projeto original não tinha:

| Capacidade | Pipeline base | Este projeto |
|---|---|---|
| Execução | Script sequencial único | **Apache Airflow**: tarefas independentes, com retry automático por etapa, agendamento nativo e rastreabilidade completa via metadados |
| Armazenamento | MinIO local (simulação) | **AWS S3 real**: dados residindo de fato na nuvem, com usuário IAM de permissão restrita e retry configurado para instabilidade de rede |

Ambas as evoluções mantêm 100% da lógica de negócio original intacta — a
resposta à pergunta "quantas filiais vs. matrizes ativas em São Paulo"
continua vindo do mesmo código de filtro e agregação; o que muda é *como*
e *onde* esse código é executado.

---

## Evolução: de MinIO local para AWS S3 real

Depois de validar a orquestração com Airflow (seção anterior), o próximo
passo foi levar o armazenamento para a nuvem de verdade. Sem alterar
nenhuma lógica de negócio — apenas trocando variáveis de ambiente:

```env
USE_MINIO=false
USE_S3=true
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
AWS_REGION=us-east-1
S3_BUCKET_NAME=nome-do-seu-bucket
```

A classe `Storage` (`src/storage.py`) abstrai três backends possíveis
(disco local, MinIO, S3 real) atrás da mesma interface — o restante do
pipeline (`ingest.py`, `process.py`, `aggregate.py`) não precisa saber
qual está em uso.

### Decisão de design: um único bucket, com prefixos por camada

Diferente do MinIO (que usa três buckets separados: bronze/silver/gold),
o modo S3 real usa **um único bucket**, com prefixos de pasta equivalentes
(`bronze/...`, `silver/...`, `gold/...`). Essa é uma prática comum em
Data Lakes reais na AWS — reduz a complexidade de gerenciar múltiplos
buckets e suas permissões.

### Segurança: usuário IAM com permissão restrita

O acesso à AWS é feito por um usuário IAM dedicado, com permissão
`AmazonS3FullAccess` — restrita ao serviço S3, sem acesso a outros
recursos da conta AWS. Segue o princípio do menor privilégio: nenhum
código do projeto usa credenciais de conta root ou de administrador.

### Desafio técnico resolvido nesta evolução

Ao migrar para S3 real, um bug de compatibilidade surgiu: a função que
lista os arquivos da camada Bronze (`src/process.py`) só tinha lógica
para os backends MinIO e disco local — faltava suporte a S3. Resolvido
centralizando essa responsabilidade em um novo método da classe `Storage`
(`list_object_names`), que lida com os três backends de forma uniforme,
eliminando a necessidade de o restante do código conhecer detalhes de
implementação de cada um.

Também foi necessário configurar retry automático no cliente `boto3`
(10 tentativas, modo adaptativo) para lidar com instabilidade de rede
durante downloads de arquivos grandes do S3 — capacidade nativa do
`boto3`, bastando configurá-la explicitamente.

### Resultado validado com S3 real

```
Matriz: 984.186
Filial: 33.035
Total: 1.017.221
```

Pipeline completo (Bronze → Silver → Gold) executado com sucesso, com
todos os dados residindo em um bucket S3 real na AWS — não mais em
simulação local.
