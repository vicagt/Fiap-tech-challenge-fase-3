# Tech Challenge Fase 3 — FIAP

Projeto desenvolvido para o Tech Challenge — Fase 3 da Pós-Tech em Data Analytics da FIAP.

O projeto utiliza três edições da pesquisa **State of Data Brasil** para construir um pipeline de Engenharia de Dados em ambiente AWS e, posteriormente, gerar análises sobre o mercado brasileiro de Dados e Inteligência Artificial.

## Objetivo

Construir uma solução de Engenharia de Dados e Analytics capaz de integrar, tratar e consolidar dados de diferentes edições da pesquisa State of Data Brasil, disponibilizando uma camada analítica para geração de indicadores, visualizações, insights e recomendações estratégicas.

## Dados utilizados

Foram utilizadas três edições da pesquisa State of Data Brasil:

- 2023–2024
- 2024–2025
- 2025–2026

Os arquivos originais em CSV são armazenados na camada Bronze do Amazon S3.

Os arquivos de dados não são versionados neste repositório. O `.gitignore` impede o versionamento de arquivos `.csv` e `.parquet`.

## Arquitetura da solução

A solução foi estruturada seguindo uma arquitetura em camadas:

**Bronze → Silver → Gold**

O fluxo implementado foi:

State of Data Brasil (CSV)  
↓  
Amazon S3 — Bronze  
↓  
AWS Glue Jobs + Apache Spark  
↓  
Amazon S3 — Silver (Parquet)  
↓  
AWS Glue Job + Apache Spark  
↓  
Amazon S3 — Gold (Parquet)  
↓  
Python + Pandas  
↓  
DataViz, Storytelling e análises  
↓  
Insights e recomendações estratégicas

O diagrama completo da arquitetura está disponível na pasta `arquitetura/`.

## Pipeline de dados

### Bronze

Armazena os arquivos CSV originais das três pesquisas, preservando os dados brutos utilizados como fonte do pipeline.

### Silver

Os dados da Bronze são processados por AWS Glue Jobs utilizando Apache Spark.

Nesta etapa são realizados tratamentos e transformações, com posterior armazenamento dos resultados em formato Parquet.

### Gold

As três pesquisas tratadas são consolidadas por meio de um novo Glue Job.

Devido às diferenças existentes entre os schemas das pesquisas, foi realizada a harmonização das estruturas antes da consolidação.

A camada Gold resultante possui **14.002 registros** e é utilizada como fonte para as análises do projeto.

## Tecnologias utilizadas

- Amazon S3
- AWS Glue
- Apache Spark / PySpark
- Python
- Pandas
- Google Colab
- Draw.io
- GitHub

## Estrutura do repositório

```text
├── arquitetura/
│   ├── README.md
│   ├── arquitetura_aws_tech_challenge_fase3.png
│   └── arquitetura_aws_tech_challenge_fase3.drawio
│
├── cadernos/
│   ├── README.md
│   ├── 01_data_understanding.ipynb
│   └── 02_analise_gold_storytelling.ipynb
│
├── documentos/
│   └── relatório executivo do projeto
│
├── src/
│   └── transformação/
│       ├── README.md
│       ├── bronze_to_silver_2023_2024.py
│       ├── bronze_to_silver_2024_2025.py
│       ├── bronze_to_silver_2025_2026.py
│       └── silver_to_gold_analytics.py
│
├── .gitignore
└── README.md
