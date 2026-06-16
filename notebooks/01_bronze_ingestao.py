# Databricks notebook source
# MAGIC %md
# MAGIC # 01 – Bronze: Ingestão das Fontes Brutas
# MAGIC
# MAGIC **Objetivo:** Ler cada fonte no formato original e persistir como Delta Table na camada Bronze, sem nenhuma transformação de conteúdo.
# MAGIC
# MAGIC A Bronze é o espelho fiel da fonte — preservamos os dados como vieram, incluindo erros, para rastreabilidade completa.
# MAGIC
# MAGIC **Premissa:** Os arquivos-fonte estão disponíveis em `SOURCES_PATH` (DBFS ou volume Unity Catalog).
# MAGIC Ajuste o caminho conforme seu ambiente antes de executar.

# COMMAND ----------

# MAGIC %md ## Configuração

# COMMAND ----------

SOURCES_PATH  = "/FileStore/sources"           # Pasta com os arquivos brutos
BRONZE_DB     = "bronze"                        # Database (schema) de destino
BRONZE_PATH   = "/delta/bronze"                 # Caminho base para os arquivos Delta

spark.sql(f"CREATE DATABASE IF NOT EXISTS {BRONZE_DB}")

# COMMAND ----------

# MAGIC %md ## Utilitário de escrita Bronze

# COMMAND ----------

def write_bronze(df, table_name: str, partition_cols: list = None):
    """Persiste um DataFrame como Delta Table na camada Bronze (overwrite)."""
    path = f"{BRONZE_PATH}/{table_name}"
    writer = (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option("path", path)
    )
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.saveAsTable(f"{BRONZE_DB}.{table_name}")
    count = spark.table(f"{BRONZE_DB}.{table_name}").count()
    print(f"[OK] {BRONZE_DB}.{table_name} → {count} linhas | path: {path}")

# COMMAND ----------

# MAGIC %md ## 1. Pedidos – Cabeçalho (CSV ; )

# COMMAND ----------

df_ped_cab = (
    spark.read
    .option("header", "true")
    .option("sep", ";")
    .option("multiLine", "true")
    .option("escape", '"')
    .option("encoding", "UTF-8")
    .csv(f"{SOURCES_PATH}/erp_pedidos_cabecalho_2025.csv")
)

# Adicionar metadados de ingestão
from pyspark.sql import functions as F

df_ped_cab = df_ped_cab.withColumn("_source_file", F.lit("erp_pedidos_cabecalho_2025.csv")) \
                        .withColumn("_ingested_at", F.current_timestamp())

write_bronze(df_ped_cab, "pedidos_cabecalho")

# COMMAND ----------

# MAGIC %md ## 2. Pedidos – Itens (CSV , )

# COMMAND ----------

df_ped_itens = (
    spark.read
    .option("header", "true")
    .option("sep", ",")
    .option("encoding", "UTF-8")
    .csv(f"{SOURCES_PATH}/erp_pedidos_itens_2025.csv")
)

df_ped_itens = df_ped_itens.withColumn("_source_file", F.lit("erp_pedidos_itens_2025.csv")) \
                            .withColumn("_ingested_at", F.current_timestamp())

write_bronze(df_ped_itens, "pedidos_itens")

# COMMAND ----------

# MAGIC %md ## 3. Produtos (JSON array aninhado)

# COMMAND ----------

df_prod = (
    spark.read
    .option("multiLine", "true")
    .json(f"{SOURCES_PATH}/cadastro_produtos_api_dump.json")
)

df_prod = df_prod.withColumn("_source_file", F.lit("cadastro_produtos_api_dump.json")) \
                  .withColumn("_ingested_at", F.current_timestamp())

write_bronze(df_prod, "produtos")

# COMMAND ----------

# MAGIC %md ## 4. Canais Comerciais (Excel .xlsx)
# MAGIC
# MAGIC > **Nota:** Requer a biblioteca `spark-excel` instalada no cluster.
# MAGIC > Em Databricks Community, instale via: `%pip install com.crealytics:spark-excel_2.12:0.14.0`

# COMMAND ----------

df_canais = (
    spark.read
    .format("com.crealytics.spark.excel")
    .option("header", "true")
    .option("dataAddress", "'canais'!A1")
    .option("inferSchema", "false")
    .load(f"{SOURCES_PATH}/comercial_canais.xlsx")
)

df_canais = df_canais.withColumn("_source_file", F.lit("comercial_canais.xlsx")) \
                      .withColumn("_ingested_at", F.current_timestamp())

write_bronze(df_canais, "canais")

# COMMAND ----------

# MAGIC %md ## 5. Clientes CRM (Excel .xlsx)

# COMMAND ----------

df_clientes = (
    spark.read
    .format("com.crealytics.spark.excel")
    .option("header", "true")
    .option("inferSchema", "false")
    .load(f"{SOURCES_PATH}/crm_clientes_export.xlsx")
)

df_clientes = df_clientes.withColumn("_source_file", F.lit("crm_clientes_export.xlsx")) \
                          .withColumn("_ingested_at", F.current_timestamp())

write_bronze(df_clientes, "clientes")

# COMMAND ----------

# MAGIC %md ## 6. Regiões (TXT pipe-delimited)

# COMMAND ----------

df_regioes = (
    spark.read
    .option("header", "true")
    .option("sep", "|")
    .option("encoding", "UTF-8")
    .csv(f"{SOURCES_PATH}/legado_regioes_pipe.txt")
)

df_regioes = df_regioes.withColumn("_source_file", F.lit("legado_regioes_pipe.txt")) \
                        .withColumn("_ingested_at", F.current_timestamp())

write_bronze(df_regioes, "regioes")

# COMMAND ----------

# MAGIC %md ## 7. Entregas Logísticas (JSON array aninhado)

# COMMAND ----------

df_entregas = (
    spark.read
    .option("multiLine", "true")
    .json(f"{SOURCES_PATH}/logistica_entregas.json")
)

df_entregas = df_entregas.withColumn("_source_file", F.lit("logistica_entregas.json")) \
                          .withColumn("_ingested_at", F.current_timestamp())

write_bronze(df_entregas, "entregas")

# COMMAND ----------

# MAGIC %md ## 8. Ocorrências de Atendimento (NDJSON – um JSON por linha)

# COMMAND ----------

df_ocorrencias = spark.read.json(f"{SOURCES_PATH}/atendimento_ocorrencias.ndjson")

df_ocorrencias = df_ocorrencias.withColumn("_source_file", F.lit("atendimento_ocorrencias.ndjson")) \
                                .withColumn("_ingested_at", F.current_timestamp())

write_bronze(df_ocorrencias, "ocorrencias")

# COMMAND ----------

# MAGIC %md ## 9. Vendedores (CSV ; )

# COMMAND ----------

df_vendedores = (
    spark.read
    .option("header", "true")
    .option("sep", ";")
    .option("encoding", "UTF-8")
    .csv(f"{SOURCES_PATH}/vendedores.csv")
)

df_vendedores = df_vendedores.withColumn("_source_file", F.lit("vendedores.csv")) \
                              .withColumn("_ingested_at", F.current_timestamp())

write_bronze(df_vendedores, "vendedores")

# COMMAND ----------

# MAGIC %md ## Inventário da Camada Bronze

# COMMAND ----------

tabelas_bronze = [
    "pedidos_cabecalho", "pedidos_itens", "produtos",
    "canais", "clientes", "regioes",
    "entregas", "ocorrencias", "vendedores"
]

print(f"{'Tabela':<30} {'Linhas':>8} {'Colunas':>8}")
print("-" * 50)
for t in tabelas_bronze:
    df = spark.table(f"{BRONZE_DB}.{t}")
    print(f"{t:<30} {df.count():>8} {len(df.columns):>8}")
