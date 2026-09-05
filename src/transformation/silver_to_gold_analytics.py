import sys

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql import functions as F
from pyspark.sql.types import StringType


# ============================================================
# 1. INICIALIZAÇÃO DO AWS GLUE / SPARK
# ============================================================

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)


# ============================================================
# 2. CAMINHOS DAS CAMADAS SILVER
# ============================================================

path_2023_2024 = (
    "s3://fiap-tech-challenge-fase3-vicagt/"
    "Silver Tratada/2023-2024/"
)

path_2024_2025 = (
    "s3://fiap-tech-challenge-fase3-vicagt/"
    "Silver Tratada/2024-2025/"
)

path_2025_2026 = (
    "s3://fiap-tech-challenge-fase3-vicagt/"
    "Silver Tratada/2025-2026/"
)


# ============================================================
# 3. CAMINHOS DA CAMADA GOLD
# ============================================================

gold_consolidada_path = (
    "s3://fiap-tech-challenge-fase3-vicagt/"
    "gold/base_consolidada/"
)

gold_resumo_path = (
    "s3://fiap-tech-challenge-fase3-vicagt/"
    "gold/resumo_periodo/"
)

gold_schema_path = (
    "s3://fiap-tech-challenge-fase3-vicagt/"
    "gold/diagnostico_schema/"
)


# ============================================================
# 4. LEITURA DAS TRÊS SILVERS
# ============================================================

df_2023 = spark.read.parquet(path_2023_2024)
df_2024 = spark.read.parquet(path_2024_2025)
df_2025 = spark.read.parquet(path_2025_2026)


# ============================================================
# 5. GARANTIA DA IDENTIFICAÇÃO DO PERÍODO
# ============================================================

def garantir_periodo(df, periodo):

    if "periodo" not in df.columns:
        df = df.withColumn(
            "periodo",
            F.lit(periodo)
        )
    else:
        df = df.withColumn(
            "periodo",
            F.coalesce(
                F.col("periodo"),
                F.lit(periodo)
            )
        )

    return df


df_2023 = garantir_periodo(
    df_2023,
    "2023-2024"
)

df_2024 = garantir_periodo(
    df_2024,
    "2024-2025"
)

df_2025 = garantir_periodo(
    df_2025,
    "2025-2026"
)


# ============================================================
# 6. CONTAGENS ORIGINAIS
# ============================================================

qtd_2023 = df_2023.count()
qtd_2024 = df_2024.count()
qtd_2025 = df_2025.count()

print("================================================")
print("REGISTROS DAS SILVERS")
print(f"2023-2024: {qtd_2023}")
print(f"2024-2025: {qtd_2024}")
print(f"2025-2026: {qtd_2025}")
print("================================================")


# ============================================================
# 7. MAPEAMENTO DOS SCHEMAS
# ============================================================

schemas = {
    "2023-2024": dict(df_2023.dtypes),
    "2024-2025": dict(df_2024.dtypes),
    "2025-2026": dict(df_2025.dtypes)
}

todas_colunas = sorted(
    set(df_2023.columns)
    | set(df_2024.columns)
    | set(df_2025.columns)
)


# ============================================================
# 8. DEFINIÇÃO DO TIPO FINAL DE CADA COLUNA
# ============================================================

#
# Regra:
#
# - Se a coluna tem o mesmo tipo em todos os anos onde existe,
#   preservamos esse tipo.
#
# - Se os tipos são diferentes entre os anos,
#   convertemos para STRING para evitar perda de informação.
#

tipos_finais = {}

for coluna in todas_colunas:

    tipos_encontrados = set()

    for schema in schemas.values():

        if coluna in schema:
            tipos_encontrados.add(
                schema[coluna]
            )

    if len(tipos_encontrados) == 1:
        tipos_finais[coluna] = list(
            tipos_encontrados
        )[0]

    else:
        tipos_finais[coluna] = "string"


# ============================================================
# 9. FUNÇÃO DE HARMONIZAÇÃO
# ============================================================

def harmonizar_dataframe(df):

    schema_atual = dict(df.dtypes)

    for coluna in todas_colunas:

        tipo_final = tipos_finais[coluna]

        if coluna not in df.columns:

            df = df.withColumn(
                coluna,
                F.lit(None).cast(tipo_final)
            )

        else:

            tipo_atual = schema_atual.get(
                coluna
            )

            if tipo_atual != tipo_final:

                df = df.withColumn(
                    coluna,
                    F.col(coluna).cast(
                        tipo_final
                    )
                )

    return df.select(
        *todas_colunas
    )


# ============================================================
# 10. HARMONIZAÇÃO DAS TRÊS BASES
# ============================================================

df_2023_h = harmonizar_dataframe(
    df_2023
)

df_2024_h = harmonizar_dataframe(
    df_2024
)

df_2025_h = harmonizar_dataframe(
    df_2025
)


# ============================================================
# 11. CONSOLIDAÇÃO HISTÓRICA
# ============================================================

gold_df = (
    df_2023_h
    .unionByName(
        df_2024_h,
        allowMissingColumns=True
    )
    .unionByName(
        df_2025_h,
        allowMissingColumns=True
    )
)


# ============================================================
# 12. REMOÇÃO DE DUPLICIDADES
# ============================================================

qtd_antes_dedup = gold_df.count()

gold_df = gold_df.dropDuplicates()

qtd_gold = gold_df.count()

duplicados_gold = (
    qtd_antes_dedup - qtd_gold
)


# ============================================================
# 13. METADADOS DA GOLD
# ============================================================

gold_df = (
    gold_df
    .withColumn(
        "camada",
        F.lit("gold")
    )
    .withColumn(
        "data_processamento_gold",
        F.current_timestamp()
    )
)


# ============================================================
# 14. VALIDAÇÕES
# ============================================================

if qtd_gold == 0:
    raise Exception(
        "Erro: Gold consolidada ficou vazia."
    )

if len(gold_df.columns) == 0:
    raise Exception(
        "Erro: Gold sem colunas."
    )


print("================================================")
print("GOLD CONSOLIDADA")
print(f"Registros: {qtd_gold}")
print(
    f"Colunas: {len(gold_df.columns)}"
)
print(
    f"Duplicados removidos: "
    f"{duplicados_gold}"
)
print("================================================")


# ============================================================
# 15. GRAVAÇÃO DA GOLD CONSOLIDADA
# ============================================================

(
    gold_df
    .write
    .mode("overwrite")
    .option(
        "compression",
        "snappy"
    )
    .parquet(
        gold_consolidada_path
    )
)


# ============================================================
# 16. RESUMO ANALÍTICO POR PERÍODO
# ============================================================

resumo_periodo = (
    gold_df
    .groupBy("periodo")
    .agg(
        F.count("*").alias(
            "quantidade_registros"
        )
    )
    .withColumn(
        "quantidade_colunas_gold",
        F.lit(
            len(gold_df.columns)
        )
    )
    .orderBy("periodo")
)


(
    resumo_periodo
    .write
    .mode("overwrite")
    .option(
        "compression",
        "snappy"
    )
    .parquet(
        gold_resumo_path
    )
)


# ============================================================
# 17. DIAGNÓSTICO DO SCHEMA
# ============================================================

diagnostico = []

for coluna in todas_colunas:

    diagnostico.append(
        (
            coluna,

            schemas[
                "2023-2024"
            ].get(
                coluna,
                "AUSENTE"
            ),

            schemas[
                "2024-2025"
            ].get(
                coluna,
                "AUSENTE"
            ),

            schemas[
                "2025-2026"
            ].get(
                coluna,
                "AUSENTE"
            ),

            tipos_finais[
                coluna
            ]
        )
    )


schema_diagnostico = (
    spark.createDataFrame(
        diagnostico,
        [
            "coluna",
            "tipo_2023_2024",
            "tipo_2024_2025",
            "tipo_2025_2026",
            "tipo_gold"
        ]
    )
)


(
    schema_diagnostico
    .write
    .mode("overwrite")
    .option(
        "header",
        "true"
    )
    .csv(
        gold_schema_path
    )
)


# ============================================================
# 18. VALIDAÇÃO FINAL
# ============================================================

print("================================================")
print("PIPELINE SILVER -> GOLD FINALIZADO")
print("================================================")

print(
    f"2023-2024: {qtd_2023} registros"
)

print(
    f"2024-2025: {qtd_2024} registros"
)

print(
    f"2025-2026: {qtd_2025} registros"
)

print(
    f"Gold consolidada: {qtd_gold} registros"
)

print(
    f"Total de colunas Gold: "
    f"{len(gold_df.columns)}"
)

print(
    "Formato Gold: Parquet + Snappy"
)

print(
    f"Gold consolidada: "
    f"{gold_consolidada_path}"
)

print(
    f"Resumo por período: "
    f"{gold_resumo_path}"
)

print(
    f"Diagnóstico de schema: "
    f"{gold_schema_path}"
)

print("================================================")


job.commit()