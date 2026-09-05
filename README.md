# Tech Challenge — Fase 3 | FIAP

Projeto desenvolvido para o **Tech Challenge — Fase 3 da Pós-Tech em Data Analytics da FIAP**.

A solução utiliza três edições da pesquisa **State of Data Brasil** para construir um pipeline de Engenharia de Dados em ambiente AWS e gerar análises sobre o mercado brasileiro de Dados e Inteligência Artificial.

## Integrantes

- **Victor Schiavone Campos — RM 370608**
- **Juliana Bahia — RM 370343**

## Objetivo

Construir uma solução de Engenharia de Dados e Analytics capaz de integrar, tratar e consolidar dados de diferentes edições da pesquisa State of Data Brasil, disponibilizando uma camada analítica para geração de indicadores, visualizações, insights e recomendações estratégicas.

## Dados utilizados

Foram utilizadas três edições da pesquisa **State of Data Brasil**:

- 2023–2024
- 2024–2025
- 2025–2026

Os arquivos originais em formato CSV foram utilizados como fonte de dados e armazenados na camada **Bronze do Amazon S3**.

Os arquivos de dados não são versionados neste repositório. O `.gitignore` impede o versionamento de arquivos `.csv` e `.parquet`, mantendo no GitHub os códigos, notebooks e documentos relacionados à implementação.

## Perguntas de negócio

As análises foram estruturadas para responder às sete perguntas propostas no Tech Challenge:

1. Como está estruturado o mercado brasileiro de Dados?
2. Quais perfis profissionais são mais valorizados pelo mercado?
3. Qual é o cenário de diversidade de gênero nas carreiras de dados?
4. Quais tecnologias apresentam maior adoção entre os profissionais?
5. Qual é o índice de adoção de Inteligência Artificial e seu impacto?
6. Existem diferenças relevantes entre regiões, senioridades ou modelos de trabalho?
7. Quais oportunidades e desafios podem ser identificados para empresas que desejam investir em Dados e Inteligência Artificial?

## Arquitetura da solução

A solução foi estruturada seguindo uma arquitetura de dados em camadas:

**Bronze → Silver → Gold**

O fluxo implementado foi:

**State of Data Brasil — CSV**  
↓  
**Amazon S3 — Bronze**  
↓  
**AWS Glue Jobs + Apache Spark**  
↓  
**Amazon S3 — Silver — Parquet**  
↓  
**AWS Glue Job + Apache Spark**  
↓  
**Amazon S3 — Gold — Parquet**  
↓  
**Python + Pandas**  
↓  
**DataViz, Storytelling e análises**  
↓  
**Insights e recomendações estratégicas**

O diagrama completo da solução está disponível na pasta `arquitetura/`.

## Pipeline de dados

### Bronze

A camada Bronze armazena os arquivos CSV originais das três edições da pesquisa, preservando os dados brutos utilizados como entrada do pipeline.

### Silver

Os dados armazenados na Bronze são processados por **AWS Glue Jobs utilizando Apache Spark/PySpark**.

Foram desenvolvidos jobs específicos para cada edição da pesquisa. Nesta etapa são realizados tratamentos e transformações dos dados, com armazenamento dos resultados em formato **Parquet** na camada Silver.

### Gold

Os dados tratados das três pesquisas são posteriormente processados por um novo AWS Glue Job.

Como as edições possuem diferenças em seus schemas, foi realizada a **harmonização das estruturas e consolidação das três pesquisas**.

A camada Gold resultante possui **14.002 registros** e constitui a base analítica utilizada nas etapas posteriores do projeto.

## Tecnologias utilizadas

- Amazon S3
- AWS Glue
- Apache Spark
- PySpark
- Python
- Pandas
- Google Colab
- Draw.io
- GitHub

## Estrutura do repositório

```text
├── arquitetura/
│   ├── README.md
│   ├── diagrama da arquitetura (.png)
│   └── arquivo editável da arquitetura (.drawio)
│
├── cadernos/
│   ├── 01_data_understanding.ipynb
│   └── 02_analise_gold_storytelling.ipynb
│
├── documentos/
│   └── relatório executivo do projeto (.pdf)
│
├── src/
│   └── transformação/
│       ├── bronze_to_silver_2023_2024.py
│       ├── bronze_to_silver_2024_2025.py
│       ├── bronze_to_silver_2025_2026.py
│       └── silver_to_gold_analytics.py
│
├── .gitignore
└── README.md
```

## Notebooks

### 01 — Data Understanding

O notebook `01_data_understanding.ipynb` contém a etapa inicial de compreensão dos dados.

Nele são exploradas as três edições da pesquisa State of Data Brasil, suas estruturas, dimensões, variáveis e particularidades, fornecendo a base para as etapas posteriores de Engenharia de Dados e Analytics.

### 02 — Análise da camada Gold, DataViz e Storytelling

O notebook `02_analise_gold_storytelling.ipynb` utiliza os dados consolidados da camada Gold.

Nele são realizadas as análises relacionadas às sete perguntas de negócio, incluindo construção de indicadores, visualizações, interpretação dos resultados, storytelling e elaboração de recomendações estratégicas.

## Processamento com AWS Glue e PySpark

Os scripts disponíveis em `src/transformação/` representam os jobs PySpark utilizados para processamento dos dados:

- `bronze_to_silver_2023_2024.py` — processamento da pesquisa 2023–2024;
- `bronze_to_silver_2024_2025.py` — processamento da pesquisa 2024–2025;
- `bronze_to_silver_2025_2026.py` — processamento da pesquisa 2025–2026;
- `silver_to_gold_analytics.py` — harmonização dos schemas e consolidação das três pesquisas na camada Gold.

Os scripts documentam as principais etapas de transformação realizadas no ambiente AWS Glue utilizando Apache Spark/PySpark.

## Análises de negócio

A camada Gold foi utilizada como fonte analítica para investigar diferentes dimensões do mercado brasileiro de Dados, incluindo:

- estrutura e composição do mercado;
- senioridade e cargos;
- valorização profissional;
- diversidade de gênero;
- tecnologias utilizadas;
- adoção de Inteligência Artificial;
- diferenças regionais;
- modelos de trabalho;
- oportunidades e desafios para organizações que investem em Dados e IA.

As análises completas, gráficos e interpretações estão disponíveis no notebook `02_analise_gold_storytelling.ipynb`.

## Principais entregáveis

O projeto está organizado em três principais componentes:

**1. Engenharia de Dados**  
Pipeline implementado em AWS utilizando Amazon S3, AWS Glue e Apache Spark/PySpark, seguindo as camadas Bronze, Silver e Gold.

**2. Analytics e Storytelling**  
Notebooks contendo Data Understanding, análise da camada Gold, indicadores, visualizações, respostas às perguntas de negócio e recomendações estratégicas.

**3. Documentação Executiva**  
Relatório executivo contendo a arquitetura da solução, principais indicadores, análises, insights e recomendações obtidas a partir dos dados.

## Arquitetura

O diagrama disponível na pasta `arquitetura/` apresenta visualmente todo o fluxo implementado, desde a ingestão das pesquisas State of Data Brasil até o consumo executivo das análises.

O arquivo é disponibilizado em formato de imagem e também no formato editável do Draw.io.

## Documentação executiva

O relatório executivo consolida os principais resultados do projeto e apresenta:

- contexto e objetivo;
- perguntas de negócio;
- arquitetura AWS;
- resultados e indicadores;
- insights;
- oportunidades e desafios;
- recomendações estratégicas;
- conclusões.

O PDF final está disponível na pasta `documentos/`.

---

### FIAP — Pós-Tech Data Analytics

**Tech Challenge — Fase 3**

**Victor Schiavone Campos — RM 370608**  
**Juliana Bahia — RM 370343**
