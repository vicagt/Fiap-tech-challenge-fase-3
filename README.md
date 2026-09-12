# Tech Challenge - Fase 3

**FIAP - Pós Tech**

**Integrantes:**
- Victor Schiavone Campos - RM 370608
- Juliana Bahia - RM 370343

## Sobre o projeto

Este repositório reúne os artefatos técnicos desenvolvidos para o **Tech Challenge - Fase 3**, cujo objetivo é analisar o mercado brasileiro de Dados, Analytics e Inteligência Artificial a partir das três edições mais recentes da pesquisa **State of Data Brasil**.

O projeto implementa uma solução de Engenharia de Dados e Analytics em ambiente AWS, utilizando arquitetura em camadas **Bronze, Silver e Gold**.

## Arquitetura e tecnologias

O pipeline foi desenvolvido utilizando:

- Amazon S3
- AWS Glue Jobs
- AWS Glue Studio Notebook
- Apache Spark / PySpark
- Python
- Pandas
- Arquitetura Medallion: Bronze, Silver e Gold
- Draw.io para documentação da arquitetura

Os dados brutos das três pesquisas foram armazenados na camada **Bronze** no Amazon S3.

Os **AWS Glue Jobs**, utilizando Apache Spark/PySpark, realizaram o tratamento e a transformação dos dados, gerando arquivos tratados em formato Parquet na camada **Silver**.

Posteriormente, os diferentes schemas das três pesquisas foram harmonizados e consolidados por meio de AWS Glue Job com Spark/PySpark, resultando na camada **Gold**.

A camada Gold foi utilizada como fonte para **consultas analíticas executadas em AWS Glue Studio Notebook com Apache Spark/PySpark**, além de alimentar as análises desenvolvidas em Python e Pandas para geração dos indicadores, visualizações, insights e recomendações estratégicas.

## Fluxo do pipeline

```text
State of Data Brasil
        ↓
Amazon S3
Camada Bronze
        ↓
AWS Glue Jobs
Apache Spark / PySpark
Tratamento e transformação
        ↓
Amazon S3
Camada Silver - Parquet
        ↓
AWS Glue Job
Apache Spark / PySpark
Harmonização dos schemas
        ↓
Amazon S3
Camada Gold
Base consolidada - 14.002 registros
        │
        ├────→ AWS Glue Studio Notebook
        │      Spark / PySpark
        │      Consultas analíticas
        │
        └────→ Python / Pandas
               DataViz e Storytelling
                       ↓
               Relatório Executivo
               Insights e Recomendações
```

## Estrutura do repositório

```text
.
├── arquitetura/
│   └── Diagrama da arquitetura AWS
│
├── cadernos/
│   ├── 01_data_understanding.ipynb
│   ├── 02_analise_gold_storytelling.ipynb
│   └── 03_consultas_analiticas_gold_glue.ipynb
│
├── documentos/
│   └── Relatório executivo do Tech Challenge
│
└── src/
    └── transformação/
        ├── bronze_to_silver_2023_2024.py
        ├── bronze_to_silver_2024_2025.py
        ├── bronze_to_silver_2025_2026.py
        └── silver_to_gold_analytics.py
```

## Processamento dos dados

O processamento foi estruturado de acordo com a arquitetura Medallion:

**Bronze:** armazenamento dos arquivos CSV originais das três edições da pesquisa State of Data Brasil no Amazon S3.

**Silver:** tratamento e transformação dos dados utilizando AWS Glue Jobs e Apache Spark/PySpark, com armazenamento dos dados tratados em formato Parquet.

**Gold:** harmonização dos diferentes schemas das pesquisas e consolidação das três edições em uma única base analítica.

A camada Gold consolidada possui **14.002 registros e 1.194 colunas**, abrangendo os três períodos analisados.

## Consultas analíticas no AWS Glue

A camada Gold foi consultada por meio do **AWS Glue Studio Notebook utilizando Apache Spark/PySpark**.

As consultas realizadas permitiram validar a estrutura da camada consolidada e analisar a distribuição dos registros entre os três períodos da pesquisa:

- **2023-2024:** 5.293 registros
- **2024-2025:** 5.215 registros
- **2025-2026:** 3.494 registros

Totalizando **14.002 registros**.

O notebook utilizado nessa etapa está disponível em:

`cadernos/03_consultas_analiticas_gold_glue.ipynb`

## Análises desenvolvidas

As análises foram direcionadas às sete perguntas de negócio propostas no desafio, contemplando:

1. Como está estruturado o mercado brasileiro de Dados?
2. Quais perfis profissionais são mais valorizados pelo mercado?
3. Qual é o cenário de diversidade de gênero nas carreiras de dados?
4. Quais tecnologias apresentam maior adoção entre os profissionais?
5. Qual é o índice de adoção de Inteligência Artificial e seu impacto?
6. Existem diferenças relevantes entre regiões, senioridades ou modelos de trabalho?
7. Quais oportunidades e desafios podem ser identificados para empresas que desejam investir em Dados e Inteligência Artificial?

As análises e visualizações foram desenvolvidas em **Python e Pandas**, utilizando a camada Gold como fonte dos dados consolidados.

## Dados

As bases utilizadas são provenientes da pesquisa **State of Data Brasil**, disponibilizada pela comunidade Data Hackers.

Os arquivos CSV originais e os arquivos Parquet gerados pelo pipeline não são versionados neste repositório.

O processamento e o armazenamento das camadas Bronze, Silver e Gold foram realizados no **Amazon S3** durante a execução do projeto.

## Resultado

O projeto implementou um pipeline de Engenharia de Dados e Analytics utilizando **Amazon S3, AWS Glue Jobs, Apache Spark/PySpark e AWS Glue Studio Notebook**, estruturado nas camadas Bronze, Silver e Gold.

As três edições da pesquisa foram consolidadas em uma camada Gold com **14.002 registros**, utilizada para consultas analíticas no AWS Glue Notebook e como fonte das análises em Python/Pandas.

A partir dos dados consolidados foram produzidos indicadores, visualizações, insights e recomendações estratégicas sobre o mercado brasileiro de Dados e Inteligência Artificial, apresentados no relatório executivo do Tech Challenge.
