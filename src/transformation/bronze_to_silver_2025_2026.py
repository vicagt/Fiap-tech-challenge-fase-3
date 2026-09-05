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
# 1. INICIALIZAÇÃO DO AWS GLUE / SPARK
# ============================================================

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)


# ============================================================
# 2. CAMINHOS DAS CAMADAS
# ============================================================

bronze_path = (
    "s3://fiap-tech-challenge-fase3-vicagt/"
    "bronze/2025-2026/"
)

silver_path = (
    "s3://fiap-tech-challenge-fase3-vicagt/"
    "Silver Tratada/2025-2026/"
)


# ============================================================
# 3. LEITURA DA CAMADA BRONZE
# ============================================================

df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("quote", '"')
    .option("escape", '"')
    .option("multiLine", "true")
    .csv(bronze_path)
)

qtd_bronze = df.count()

print("================================================")
print("LEITURA DA BRONZE - 2025-2026")
print(f"Registros encontrados: {qtd_bronze}")
print(f"Quantidade de colunas: {len(df.columns)}")
print("================================================")


# ============================================================
# 4. PADRONIZAÇÃO DOS NOMES DAS COLUNAS
# ============================================================

def normalizar_nome_coluna(nome):

    # Remove acentos
    nome = unicodedata.normalize("NFKD", nome)
    nome = "".join(
        caractere
        for caractere in nome
        if not unicodedata.combining(caractere)
    )

    # Converte para minúsculo
    nome = nome.lower().strip()

    # Substitui caracteres especiais por _
    nome = re.sub(r"[^a-z0-9]+", "_", nome)

    # Remove _ no começo/final
    nome = nome.strip("_")

    return nome


novos_nomes = []
nomes_utilizados = {}

for coluna in df.columns:

    novo_nome = normalizar_nome_coluna(coluna)

    # Evita nomes duplicados após a normalização
    if novo_nome in nomes_utilizados:
        nomes_utilizados[novo_nome] += 1
        novo_nome = (
            f"{novo_nome}_{nomes_utilizados[novo_nome]}"
        )
    else:
        nomes_utilizados[novo_nome] = 0

    novos_nomes.append(novo_nome)


df = df.toDF(*novos_nomes)


# ============================================================
# 5. LIMPEZA DAS COLUNAS DE TEXTO
# ============================================================

valores_nulos = [
    "",
    "null",
    "none",
    "n/a",
    "na",
    "nan",
    "undefined"
]

for campo in df.schema.fields:

    if isinstance(campo.dataType, StringType):

        coluna = campo.name

        # Remove espaços no início/fim
        df = df.withColumn(
            coluna,
            F.trim(F.col(coluna))
        )

        # Converte representações textuais de nulo em NULL real
        df = df.withColumn(
            coluna,
            F.when(
                F.lower(F.col(coluna)).isin(valores_nulos),
                F.lit(None)
            ).otherwise(F.col(coluna))
        )


# ============================================================
# 6. REMOÇÃO DE DUPLICIDADES
# ============================================================

qtd_antes_duplicados = df.count()

df = df.dropDuplicates()

qtd_depois_duplicados = df.count()

duplicados_removidos = (
    qtd_antes_duplicados - qtd_depois_duplicados
)

print("================================================")
print("TRATAMENTO DE DUPLICIDADES")
print(f"Antes: {qtd_antes_duplicados}")
print(f"Depois: {qtd_depois_duplicados}")
print(f"Duplicados removidos: {duplicados_removidos}")
print("================================================")


# ============================================================
# 7. METADADOS DO PROCESSAMENTO
# ============================================================

df = (
    df
    .withColumn(
        "periodo",
        F.lit("2025-2026")
    )
    .withColumn(
        "data_processamento",
        F.current_timestamp()
    )
)


# ============================================================
# 8. VALIDAÇÕES DE QUALIDADE
# ============================================================

qtd_silver = df.count()
qtd_colunas = len(df.columns)

if qtd_colunas == 0:
    raise Exception(
        "Erro de qualidade: nenhuma coluna encontrada "
        "após o tratamento."
    )

if qtd_silver == 0:
    raise Exception(
        "Erro de qualidade: nenhum registro disponível "
        "após o tratamento."
    )


print("================================================")
print("SCHEMA FINAL DA SILVER")
df.printSchema()
print("================================================")


# ============================================================
# 9. GRAVAÇÃO NA SILVER TRATADA
# ============================================================

(
    df.write
    .mode("overwrite")
    .option("compression", "snappy")
    .parquet(silver_path)
)


print("================================================")
print("CAMADA SILVER 2025-2026 GERADA COM SUCESSO")
print(f"Destino: {silver_path}")
print(f"Registros finais: {qtd_silver}")
print(f"Colunas finais: {qtd_colunas}")
print("Formato: Parquet")
print("Compressão: Snappy")
print("================================================")


# ============================================================
# 10. FINALIZAÇÃO DO JOB
# ============================================================

job.commit()