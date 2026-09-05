import sys
import re
import unicodedata

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# ============================================================
# INICIALIZAÇÃO DO AWS GLUE / SPARK
# ============================================================

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ============================================================
# CAMINHOS DAS CAMADAS
# ============================================================

bronze_path = (
    "s3://fiap-tech-challenge-fase3-vicagt/"
    "bronze/2023-2024/"
)

silver_path = (
    "s3://fiap-tech-challenge-fase3-vicagt/"
    "Silver Tratada/2023-2024/"
)

# ============================================================
# 1. LEITURA DA CAMADA BRONZE
# ============================================================

df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("quote", '"')
    .option("escape", '"')
    .csv(bronze_path)
)

qtd_bronze = df.count()

print("==============================================")
print("CAMADA BRONZE")
print(f"Quantidade inicial de registros: {qtd_bronze}")
print(f"Quantidade inicial de colunas: {len(df.columns)}")
print("==============================================")

# ============================================================
# 2. PADRONIZAÇÃO DOS NOMES DAS COLUNAS
# ============================================================

def normalize_column_name(name):
    # Remove acentos
    name = unicodedata.normalize("NFKD", name)
    name = name.encode("ascii", "ignore").decode("ascii")

    # Padronização
    name = name.strip().lower()

    # Caracteres especiais -> _
    name = re.sub(r"[^a-z0-9]+", "_", name)

    # Remove repetições de _
    name = re.sub(r"_+", "_", name)

    # Remove _ no início/fim
    name = name.strip("_")

    # Evita coluna iniciando por número
    if name and name[0].isdigit():
        name = "col_" + name

    return name


# Garante nomes únicos após a normalização
used_names = {}

for old_name in df.columns:
    base_name = normalize_column_name(old_name)

    if not base_name:
        base_name = "coluna"

    if base_name not in used_names:
        used_names[base_name] = 1
        new_name = base_name
    else:
        used_names[base_name] += 1
        new_name = f"{base_name}_{used_names[base_name]}"

    if old_name != new_name:
        df = df.withColumnRenamed(old_name, new_name)

# ============================================================
# 3. LIMPEZA DAS COLUNAS DE TEXTO
# ============================================================

null_values = ["", "nan", "null", "none", "n/a", "na"]

for field in df.schema.fields:
    if isinstance(field.dataType, StringType):
        col_name = field.name

        # Remove espaços extras no início/fim
        df = df.withColumn(
            col_name,
            F.trim(F.col(col_name))
        )

        # Converte valores textuais de ausência para NULL real
        df = df.withColumn(
            col_name,
            F.when(
                F.lower(F.trim(F.col(col_name))).isin(null_values),
                F.lit(None)
            ).otherwise(F.col(col_name))
        )

# ============================================================
# 4. REMOÇÃO DE DUPLICIDADES
# ============================================================

# É feita após a limpeza para eliminar também registros que
# eram diferentes apenas por espaços ou valores inconsistentes.
df = df.dropDuplicates()

qtd_silver = df.count()
duplicados_removidos = qtd_bronze - qtd_silver

print("==============================================")
print("DEDUPLICAÇÃO")
print(f"Registros após tratamento: {qtd_silver}")
print(f"Duplicados removidos: {duplicados_removidos}")
print("==============================================")

# ============================================================
# 5. METADADOS DA CAMADA SILVER
# ============================================================

df = (
    df
    .withColumn("periodo", F.lit("2023-2024"))
    .withColumn(
        "data_processamento",
        F.current_timestamp()
    )
)

# ============================================================
# 6. VALIDAÇÕES BÁSICAS
# ============================================================

if len(df.columns) == 0:
    raise Exception(
        "Erro de qualidade: DataFrame sem colunas."
    )

if qtd_silver == 0:
    raise Exception(
        "Erro de qualidade: nenhum registro disponível "
        "após o tratamento."
    )

print("==============================================")
print("SCHEMA FINAL DA SILVER")
df.printSchema()
print("==============================================")

# ============================================================
# 7. GRAVAÇÃO NA SILVER TRATADA
# ============================================================

(
    df.write
    .mode("overwrite")
    .option("compression", "snappy")
    .parquet(silver_path)
)

print("==============================================")
print("CAMADA SILVER GERADA COM SUCESSO")
print(f"Destino: {silver_path}")
print(f"Registros finais: {qtd_silver}")
print("Formato: Parquet")
print("Compressão: Snappy")
print("==============================================")

job.commit()