# Databricks notebook source
# MAGIC %md
# MAGIC # 02 – Silver: Limpeza e Padronização
# MAGIC
# MAGIC **Objetivo:** Transformar os dados Bronze em registros limpos, padronizados e confiáveis.
# MAGIC
# MAGIC Cada entidade recebe tratamento específico conforme os problemas identificados na exploração:
# MAGIC - Datas com múltiplos formatos → parsing defensivo com `coalesce`
# MAGIC - Campos de texto com case inconsistente → `upper` / `lower` / `initcap`
# MAGIC - Decimais com vírgula → `regexp_replace`
# MAGIC - Duplicatas → deduplição com `row_number` por chave primária
# MAGIC - IDs em lowercase → `upper(trim(...))`
# MAGIC - Registros inválidos → filtro com documentação da decisão

# COMMAND ----------

from pyspark.sql import functions as F, Window
from pyspark.sql.types import *

BRONZE_DB = "bronze"
SILVER_DB = "silver"
SILVER_PATH = "/delta/silver"

spark.sql(f"CREATE DATABASE IF NOT EXISTS {SILVER_DB}")

# COMMAND ----------

def write_silver(df, table_name: str, partition_cols: list = None):
    """Persiste um DataFrame como Delta Table na camada Silver."""
    path = f"{SILVER_PATH}/{table_name}"
    writer = (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option("path", path)
    )
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.saveAsTable(f"{SILVER_DB}.{table_name}")
    count = spark.table(f"{SILVER_DB}.{table_name}").count()
    print(f"[OK] {SILVER_DB}.{table_name} → {count} linhas | path: {path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Utilitário: Parsing de Data Multi-formato
# MAGIC
# MAGIC As fontes usam três formatos distintos. Tentamos cada um em cascata com `coalesce`;
# MAGIC se nenhum funcionar, o resultado é `null` e o registro é sinalizado.

# COMMAND ----------

def parse_date_multiformat(col_name: str, alias: str):
    """
    Tenta converter para date nos formatos:
      1. yyyy-MM-dd
      2. yyyy/MM/dd
      3. dd/MM/yyyy
    Retorna null se nenhum casar.
    """
    return F.coalesce(
        F.to_date(F.col(col_name), "yyyy-MM-dd"),
        F.to_date(F.col(col_name), "yyyy/MM/dd"),
        F.to_date(F.col(col_name), "dd/MM/yyyy"),
    ).alias(alias)

def parse_datetime_multiformat(col_name: str, alias: str):
    """
    Tenta converter para timestamp nos formatos:
      1. yyyy-MM-dd HH:mm:ss
      2. yyyy/MM/dd
      3. dd/MM/yyyy HH:mm
      4. dd/MM/yyyy HH:mm:ss
      5. yyyy-MM-dd (sem hora)
    """
    return F.coalesce(
        F.to_timestamp(F.col(col_name), "yyyy-MM-dd HH:mm:ss"),
        F.to_timestamp(F.col(col_name), "yyyy/MM/dd"),
        F.to_timestamp(F.col(col_name), "dd/MM/yyyy HH:mm"),
        F.to_timestamp(F.col(col_name), "dd/MM/yyyy HH:mm:ss"),
        F.to_timestamp(F.col(col_name), "yyyy-MM-dd"),
    ).alias(alias)

# COMMAND ----------

# MAGIC %md ## 1. Regiões (base para joins posteriores)
# MAGIC
# MAGIC **Decisões:**
# MAGIC - Remover `XX` (active_flag=0, sem dados úteis)
# MAGIC - `sul` é duplicata de `S`; manter apenas o registro canônico (`S`)
# MAGIC - `SE` tem duas linhas com estados diferentes; manter a com estado = `SP` (mais completo)
# MAGIC - Normalizar `regional_code` para UPPERCASE
# MAGIC - Normalizar `state` para UPPERCASE (sigla de 2 letras)

# COMMAND ----------

df_reg_raw = spark.table(f"{BRONZE_DB}.regioes")

# Mapeamento de estados por extenso para sigla
estado_map = {
    "AM": "AM", "BA": "BA", "SP": "SP", "SC": "SC", "GO": "GO",
    "sao paulo": "SP", "Sta Catarina": "SC", "santa catarina": "SC",
    "Paraná": "PR", "PR": "PR", "RJ": "RJ", "MG": "MG",
    "": None
}

# Normalizar e deduplicar
# Prioridade: registros com active_flag=1 e state não nulo
w_reg = Window.partitionBy(F.upper(F.trim(F.col("regional_code")))).orderBy(
    F.desc("active_flag"),
    F.when(F.col("state").isNull() | (F.trim(F.col("state")) == ""), 1).otherwise(0)
)

df_regioes = (
    df_reg_raw
    # Normalizar regional_code
    .withColumn("regional_code", F.upper(F.trim(F.col("regional_code"))))
    # Mapear 'SUL' → 'S' (alias legado)
    .withColumn("regional_code",
        F.when(F.col("regional_code") == "SUL", F.lit("S")).otherwise(F.col("regional_code"))
    )
    # Excluir inválidos
    .filter(
        (F.col("regional_code").isNotNull()) &
        (F.col("regional_code") != "") &
        (F.col("regional_code") != "XX")
    )
    # Normalizar state para uppercase 2 chars (simples - estados já são siglas ou nomes conhecidos)
    .withColumn("state", F.upper(F.trim(F.col("state"))))
    # Corrigir nomes de estado por extenso
    .withColumn("state",
        F.when(F.col("state").isin("SAO PAULO"), "SP")
        .when(F.col("state").isin("STA CATARINA", "SANTA CATARINA"), "SC")
        .when(F.col("state").isin("PARANA", "PARANÁ"), "PR")
        .otherwise(F.col("state"))
    )
    .withColumn("active_flag", F.col("active_flag").cast("integer"))
    .withColumn("rn", F.row_number().over(w_reg))
    .filter(F.col("rn") == 1)
    .drop("rn", "_source_file", "_ingested_at")
)

write_silver(df_regioes, "regioes")
df_regioes.show()

# COMMAND ----------

# MAGIC %md ## 2. Canais Comerciais
# MAGIC
# MAGIC **Decisões:**
# MAGIC - Normalizar `id_canal` para UPPERCASE
# MAGIC - `CH05` duplicado: manter primeira ocorrência (`E-commerce`) — logar conflito
# MAGIC - `CH06` sem nome: manter com `nome_canal = 'Desconhecido'`
# MAGIC - `CH04` inativo (`nao`): manter na dimensão com flag `ativo = false`
# MAGIC - Normalizar `ativo` para boolean

# COMMAND ----------

df_can_raw = spark.table(f"{BRONZE_DB}.canais")

# Detectar e logar duplicatas antes de deduplicar
print("=== Conflitos detectados em canais ===")
df_can_raw \
    .withColumn("id_canal_norm", F.upper(F.trim(F.col("id_canal")))) \
    .groupBy("id_canal_norm") \
    .count().filter("count > 1") \
    .show()

w_can = Window.partitionBy(F.upper(F.trim(F.col("id_canal")))).orderBy(F.monotonically_increasing_id())

df_canais = (
    df_can_raw
    .withColumn("id_canal", F.upper(F.trim(F.col("id_canal"))))
    .withColumn("nome_canal",
        F.when(F.col("nome_canal").isNull() | (F.trim(F.col("nome_canal")) == ""), F.lit("Desconhecido"))
        .otherwise(F.initcap(F.trim(F.col("nome_canal"))))
    )
    .withColumn("tipo_canal", F.initcap(F.trim(F.col("tipo_canal"))))
    .withColumn("ativo",
        F.when(F.lower(F.trim(F.col("ativo"))).isin("sim", "s", "true", "1"), F.lit(True))
        .when(F.lower(F.trim(F.col("ativo"))).isin("nao", "não", "n", "false", "0"), F.lit(False))
        .otherwise(F.lit(None).cast("boolean"))
    )
    .withColumn("rn", F.row_number().over(w_can))
    .filter(F.col("rn") == 1)
    .drop("rn", "observacao", "_source_file", "_ingested_at")
)

write_silver(df_canais, "canais")
df_canais.show()

# COMMAND ----------

# MAGIC %md ## 3. Vendedores
# MAGIC
# MAGIC **Decisões:**
# MAGIC - V004: dois registros com canal diferente. `CH99` não existe na tabela de canais → manter o com `CH02`
# MAGIC - V008: duplicata com nome diferente → manter `Vendedor 8` (primeira ocorrência, nome sem sufixo)
# MAGIC - `regional_code = sul/SUL` → normalizar para `S`
# MAGIC - `canal_id` em lowercase → normalizar para UPPERCASE
# MAGIC - `hire_date` multi-formato → parsear para date
# MAGIC - `status` com case inconsistente → normalizar para `Ativo`/`Inativo`

# COMMAND ----------

df_vend_raw = spark.table(f"{BRONZE_DB}.vendedores")

# Log de duplicatas
print("=== Duplicatas em vendedores ===")
df_vend_raw.groupBy("seller_id").count().filter("count > 1").show()

# Para V004: manter o com canal_id existente nos canais (CH02, não CH99)
# Para V008: manter o sem sufixo "duplicado" no nome
# Estratégia: ordenar por canal_id válido (não CH99) e nome sem "duplicado"
w_vend = Window.partitionBy(F.col("seller_id")).orderBy(
    # Priorizar registros com canal_id diferente de CH99
    F.when(F.upper(F.trim(F.col("canal_id"))) == "CH99", 1).otherwise(0),
    # Priorizar nomes sem "duplicado"
    F.when(F.lower(F.col("seller_name")).contains("duplicado"), 1).otherwise(0),
    F.monotonically_increasing_id()
)

df_vendedores = (
    df_vend_raw
    .withColumn("seller_id", F.upper(F.trim(F.col("seller_id"))))
    .withColumn("canal_id", F.upper(F.trim(F.col("canal_id"))))
    # Normalizar regional_code: sul → S
    .withColumn("regional_code",
        F.when(F.lower(F.trim(F.col("regional_code"))) == "sul", F.lit("S"))
        .otherwise(F.upper(F.trim(F.col("regional_code"))))
    )
    # Parsear hire_date
    .withColumn("hire_date", parse_date_multiformat("hire_date", "hire_date"))
    # Normalizar status
    .withColumn("status",
        F.when(F.lower(F.trim(F.col("status"))).isin("ativo", "active"), F.lit("Ativo"))
        .when(F.lower(F.trim(F.col("status"))).isin("inativo", "inactive"), F.lit("Inativo"))
        .otherwise(F.lit("Indefinido"))
    )
    .withColumn("rn", F.row_number().over(w_vend))
    .filter(F.col("rn") == 1)
    .drop("rn", "_source_file", "_ingested_at")
)

print(f"\nLinhas após deduplicação: {df_vendedores.count()} (de {df_vend_raw.count()} originais)")
write_silver(df_vendedores, "vendedores")

# COMMAND ----------

# MAGIC %md ## 4. Produtos
# MAGIC
# MAGIC **Decisões:**
# MAGIC - Achatar estrutura aninhada (`product`, `pricing`, `attributes`)
# MAGIC - Normalizar `status` para `Ativo`/`Inativo`/`Descontinuado`
# MAGIC - `tags` (array) → concatenado em string separada por `|`

# COMMAND ----------

df_prod_raw = spark.table(f"{BRONZE_DB}.produtos")

df_produtos = (
    df_prod_raw
    .select(
        F.col("product.product_id").alias("product_id"),
        F.col("product.name").alias("product_name"),
        F.col("product.category").alias("category"),
        F.col("product.subcategory").alias("subcategory"),
        F.initcap(F.trim(F.col("product.status"))).alias("status"),
        F.col("pricing.list_price").alias("list_price"),
        F.col("pricing.currency").alias("currency"),
        F.col("attributes.family").alias("family"),
        F.concat_ws("|", F.col("attributes.tags")).alias("tags"),
        F.to_timestamp(F.col("updated_at")).alias("updated_at"),
    )
    # Normalizar status com case inconsistente
    .withColumn("status",
        F.when(F.lower(F.col("status")).isin("ativo"), "Ativo")
        .when(F.lower(F.col("status")).isin("inativo"), "Inativo")
        .when(F.lower(F.col("status")).isin("descontinuado"), "Descontinuado")
        .otherwise("Indefinido")
    )
)

write_silver(df_produtos, "produtos")
df_produtos.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md ## 5. Clientes
# MAGIC
# MAGIC **Decisões:**
# MAGIC - Normalizar `estado`: nome por extenso → sigla de 2 letras (UF)
# MAGIC - `data_cadastro` multi-formato → parsear para date
# MAGIC - `segmento` nulo → `Não Informado`
# MAGIC - `status_cliente` nulo → `Não Informado`
# MAGIC - `porte` com case inconsistente → `initcap`

# COMMAND ----------

df_cli_raw = spark.table(f"{BRONZE_DB}.clientes")

# Mapeamento de estados (nome → sigla)
estado_to_uf_map = {
    "acre": "AC", "alagoas": "AL", "amapa": "AP", "amazonas": "AM",
    "bahia": "BA", "ceara": "CE", "distrito federal": "DF",
    "espirito santo": "ES", "goias": "GO", "maranhao": "MA",
    "mato grosso": "MT", "mato grosso do sul": "MS", "minas gerais": "MG",
    "para": "PA", "paraiba": "PB", "parana": "PR", "pernambuco": "PE",
    "piaui": "PI", "rio de janeiro": "RJ", "rio grande do norte": "RN",
    "rio grande do sul": "RS", "rondonia": "RO", "roraima": "RR",
    "santa catarina": "SC", "sao paulo": "SP", "sergipe": "SE",
    "tocantins": "TO",
    # Variações encontradas nos dados
    "minas": "MG",
}

def normalize_estado(col_name):
    """Normaliza nome de estado por extenso para sigla UF."""
    c = F.lower(F.trim(F.regexp_replace(F.col(col_name), "[áàãâ]", "a")
                              .alias("_tmp")))
    # Se já for sigla de 2 chars, returna uppercase; senão mapeia
    expr = F.when(F.length(F.trim(F.col(col_name))) == 2,
                  F.upper(F.trim(F.col(col_name))))
    for nome, uf in estado_to_uf_map.items():
        expr = expr.when(
            F.lower(F.trim(F.col(col_name))).contains(nome[:5]),
            F.lit(uf)
        )
    return expr.otherwise(F.upper(F.trim(F.col(col_name))))

df_clientes = (
    df_cli_raw
    .withColumn("data_cadastro", parse_date_multiformat("data_cadastro", "data_cadastro"))
    .withColumn("updated_at", F.to_timestamp(F.col("updated_at"), "yyyy-MM-dd HH:mm:ss"))
    .withColumn("estado", normalize_estado("estado"))
    .withColumn("segmento",
        F.when(F.col("segmento").isNull() | (F.trim(F.col("segmento")) == ""), F.lit("Não Informado"))
        .otherwise(F.col("segmento"))
    )
    .withColumn("status_cliente",
        F.when(F.col("status_cliente").isNull() | (F.trim(F.col("status_cliente")) == ""), F.lit("Não Informado"))
        .otherwise(F.initcap(F.trim(F.col("status_cliente"))))
    )
    .withColumn("porte",
        F.when(F.col("porte").isNull() | (F.trim(F.col("porte")) == ""), F.lit("Não Informado"))
        .otherwise(F.initcap(F.trim(F.col("porte"))))
    )
    .drop("_source_file", "_ingested_at")
)

write_silver(df_clientes, "clientes")
df_clientes.show(5)

# COMMAND ----------

# MAGIC %md ## 6. Pedidos – Cabeçalho
# MAGIC
# MAGIC **Decisões:**
# MAGIC - `order_date` e `promised_date` multi-formato → parsear para date
# MAGIC - `status_order` normalizar para valores canônicos:
# MAGIC   - `Faturado`, `faturado` → `FATURADO`
# MAGIC   - `EM_SEPARACAO`, `em separacao` → `EM_SEPARACAO`
# MAGIC   - `cancelado` → `CANCELADO`
# MAGIC   - `entregue` → `ENTREGUE`
# MAGIC   - vazio/null → `INDEFINIDO`
# MAGIC - `payment_details` é JSON em string → parsear campos `priority` e `source`
# MAGIC - Remover espaços em customer_code e seller_id

# COMMAND ----------

df_ped_raw = spark.table(f"{BRONZE_DB}.pedidos_cabecalho")

df_pedidos_cab = (
    df_ped_raw
    .withColumn("order_id", F.upper(F.trim(F.col("order_id"))))
    .withColumn("customer_code", F.upper(F.trim(F.col("customer_code"))))
    .withColumn("seller_id", F.upper(F.trim(F.col("seller_id"))))
    .withColumn("order_date",       parse_date_multiformat("order_date",     "order_date"))
    .withColumn("promised_date",    parse_date_multiformat("promised_date",  "promised_date"))
    .withColumn("last_update",      F.to_timestamp(F.col("last_update"), "yyyy-MM-dd HH:mm:ss"))
    # Normalizar status
    .withColumn("status_order",
        F.when(F.lower(F.trim(F.col("status_order"))).isin("faturado"),           F.lit("FATURADO"))
        .when(F.lower(F.trim(F.col("status_order"))).isin("em_separacao", "em separacao"), F.lit("EM_SEPARACAO"))
        .when(F.lower(F.trim(F.col("status_order"))).isin("cancelado"),            F.lit("CANCELADO"))
        .when(F.lower(F.trim(F.col("status_order"))).isin("entregue"),             F.lit("ENTREGUE"))
        .otherwise(F.lit("INDEFINIDO"))
    )
    # Cast numéricos
    .withColumn("gross_amount",    F.col("gross_amount").cast("double"))
    .withColumn("discount_amount", F.col("discount_amount").cast("double"))
    .withColumn("net_amount",      F.col("net_amount").cast("double"))
    # Extrair campos do JSON de payment_details
    .withColumn("payment_priority",
        F.get_json_object(F.col("payment_details"), "$.priority")
    )
    .withColumn("payment_source",
        F.get_json_object(F.col("payment_details"), "$.source")
    )
    .drop("payment_details", "_source_file", "_ingested_at")
)

# Validação: pedidos com data inválida
invalid_dates = df_pedidos_cab.filter(F.col("order_date").isNull())
print(f"Pedidos com order_date não parseável: {invalid_dates.count()}")
invalid_dates.select("order_id", "_source_file").show() if "_source_file" in df_ped_raw.columns else None

write_silver(df_pedidos_cab, "pedidos_cabecalho")
df_pedidos_cab.show(5)

# COMMAND ----------

# MAGIC %md ## 7. Pedidos – Itens
# MAGIC
# MAGIC **Decisões:**
# MAGIC - Normalizar `order_id` para UPPERCASE (corrige `o00177` → `O00177`)
# MAGIC - Substituir vírgula por ponto em `unit_price` e `total_item` antes do cast
# MAGIC - `item_status` normalizar: vazio → `Indefinido`
# MAGIC - Flag `sem_cabecalho`: itens cujo `order_id` não existe no cabeçalho após normalização

# COMMAND ----------

df_itens_raw = spark.table(f"{BRONZE_DB}.pedidos_itens")

# Obter conjunto de order_ids válidos do cabeçalho
orders_validos = df_pedidos_cab.select("order_id").distinct()

df_pedidos_itens = (
    df_itens_raw
    .withColumn("order_id", F.upper(F.trim(F.col("order_id"))))
    .withColumn("product_code", F.upper(F.trim(F.col("product_code"))))
    .withColumn("item_seq", F.col("item_seq").cast("integer"))
    .withColumn("quantity",
        F.regexp_replace(F.col("quantity"), ",", ".").cast("double")
    )
    .withColumn("unit_price",
        F.regexp_replace(F.col("unit_price"), ",", ".").cast("double")
    )
    .withColumn("total_item",
        F.regexp_replace(F.col("total_item"), ",", ".").cast("double")
    )
    # Recomputar total_item quando difere de quantity * unit_price (tolerância 0.01)
    .withColumn("total_item_calc", F.round(F.col("quantity") * F.col("unit_price"), 2))
    .withColumn("total_item_flag",
        F.when(F.abs(F.col("total_item") - F.col("total_item_calc")) > 0.01,
               F.lit("DIVERGENCIA_TOTAL"))
        .otherwise(F.lit(None).cast("string"))
    )
    # Usar valor calculado quando há divergência (mais confiável)
    .withColumn("total_item",
        F.when(F.col("total_item_flag") == "DIVERGENCIA_TOTAL", F.col("total_item_calc"))
        .otherwise(F.col("total_item"))
    )
    .withColumn("item_status",
        F.when(F.col("item_status").isNull() | (F.trim(F.col("item_status")) == ""), F.lit("Indefinido"))
        .otherwise(F.initcap(F.trim(F.col("item_status"))))
    )
    .drop("total_item_calc", "_source_file", "_ingested_at")
)

# Marcar itens órfãos
df_pedidos_itens = (
    df_pedidos_itens
    .join(orders_validos.withColumn("_existe", F.lit(True)), "order_id", "left")
    .withColumn("sem_cabecalho", F.when(F.col("_existe").isNull(), F.lit(True)).otherwise(F.lit(False)))
    .drop("_existe")
)

print(f"Itens com divergência em total_item: {df_pedidos_itens.filter(F.col('total_item_flag').isNotNull()).count()}")
print(f"Itens sem cabeçalho: {df_pedidos_itens.filter(F.col('sem_cabecalho')).count()}")

write_silver(df_pedidos_itens, "pedidos_itens")

# COMMAND ----------

# MAGIC %md ## 8. Entregas
# MAGIC
# MAGIC **Decisões:**
# MAGIC - Achatar estrutura aninhada (`carrier`, `timestamps`, `destination`)
# MAGIC - Normalizar `delivery_status` para lowercase
# MAGIC - Normalizar `carrier.mode` para `initcap`
# MAGIC - Flag `order_sem_cabecalho` para entregas cujo `order_ref` não consta no cabeçalho
# MAGIC - `delivered_at` nulo para status `in_transit` / `atrasado` é esperado

# COMMAND ----------

df_ent_raw = spark.table(f"{BRONZE_DB}.entregas")

df_entregas = (
    df_ent_raw
    .select(
        F.col("delivery_id"),
        F.upper(F.trim(F.col("order_ref"))).alias("order_ref"),
        F.col("carrier.name").alias("carrier_name"),
        F.initcap(F.trim(F.col("carrier.mode"))).alias("carrier_mode"),
        F.lower(F.trim(F.col("delivery_status"))).alias("delivery_status"),
        F.to_timestamp(F.col("timestamps.shipped_at"),   "dd/MM/yyyy HH:mm").alias("shipped_at"),
        F.to_timestamp(F.col("timestamps.delivered_at"), "dd/MM/yyyy HH:mm").alias("delivered_at"),
        F.col("destination.state").alias("dest_state"),
        F.col("destination.city").alias("dest_city"),
        F.col("cost").alias("delivery_cost"),
    )
    .withColumn("carrier_mode",
        F.when(F.lower(F.col("carrier_mode")).contains("rodo"), F.lit("Rodoviário"))
        .when(F.lower(F.col("carrier_mode")).contains("a"), F.lit("Aéreo"))
        .otherwise(F.col("carrier_mode"))
    )
    .withColumn("delivery_status",
        F.when(F.col("delivery_status").isNull(), F.lit("indefinido"))
        .otherwise(F.col("delivery_status"))
    )
)

# Flag entregas sem pedido no cabeçalho
df_entregas = (
    df_entregas
    .join(orders_validos.withColumnRenamed("order_id", "order_ref")
                         .withColumn("_existe", F.lit(True)), "order_ref", "left")
    .withColumn("order_sem_cabecalho",
        F.when(F.col("_existe").isNull(), F.lit(True)).otherwise(F.lit(False))
    )
    .drop("_existe")
)

print(f"Entregas sem pedido no cabeçalho: {df_entregas.filter(F.col('order_sem_cabecalho')).count()}")
write_silver(df_entregas, "entregas")
df_entregas.groupBy("delivery_status").count().show()

# COMMAND ----------

# MAGIC %md ## 9. Ocorrências de Atendimento
# MAGIC
# MAGIC **Decisões:**
# MAGIC - `created_at` multi-formato → parsear para timestamp
# MAGIC - `event_type`, `status`, `severity` → normalizar para lowercase
# MAGIC - Nulos em `event_type` → `indefinido`
# MAGIC - Flag `order_sem_cabecalho` para ocorrências cujo `order_id` não consta

# COMMAND ----------

df_oc_raw = spark.table(f"{BRONZE_DB}.ocorrencias")

df_ocorrencias = (
    df_oc_raw
    .withColumn("order_id", F.upper(F.trim(F.col("order_id"))))
    .withColumn("created_at", parse_datetime_multiformat("created_at", "created_at"))
    .withColumn("event_type",
        F.when(F.col("event_type").isNull() | (F.trim(F.col("event_type")) == ""), F.lit("indefinido"))
        .otherwise(F.lower(F.trim(F.col("event_type"))))
    )
    .withColumn("status",
        F.when(F.col("status").isNull() | (F.trim(F.col("status")) == ""), F.lit("indefinido"))
        .otherwise(F.lower(F.trim(F.col("status"))))
    )
    .withColumn("severity",
        F.when(F.col("severity").isNull() | (F.trim(F.col("severity")) == ""), F.lit("indefinido"))
        .otherwise(F.lower(F.trim(F.col("severity"))))
    )
    .drop("_source_file", "_ingested_at")
)

# Flag ocorrências sem pedido no cabeçalho
df_ocorrencias = (
    df_ocorrencias
    .join(orders_validos.withColumn("_existe", F.lit(True)), "order_id", "left")
    .withColumn("order_sem_cabecalho",
        F.when(F.col("_existe").isNull(), F.lit(True)).otherwise(F.lit(False))
    )
    .drop("_existe")
)

print(f"Ocorrências sem pedido no cabeçalho: {df_ocorrencias.filter(F.col('order_sem_cabecalho')).count()}")
write_silver(df_ocorrencias, "ocorrencias")
df_ocorrencias.groupBy("event_type").count().show()

# COMMAND ----------

# MAGIC %md ## Inventário da Camada Silver

# COMMAND ----------

tabelas_silver = [
    "regioes", "canais", "vendedores", "produtos",
    "clientes", "pedidos_cabecalho", "pedidos_itens",
    "entregas", "ocorrencias"
]

print(f"{'Tabela':<25} {'Linhas':>8} {'Colunas':>8}")
print("-" * 45)
for t in tabelas_silver:
    df = spark.table(f"{SILVER_DB}.{t}")
    print(f"{t:<25} {df.count():>8} {len(df.columns):>8}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validações Pós-Silver
# MAGIC
# MAGIC Checagem de integridade referencial entre as entidades principais.

# COMMAND ----------

df_ped_sil  = spark.table(f"{SILVER_DB}.pedidos_cabecalho")
df_cli_sil  = spark.table(f"{SILVER_DB}.clientes")
df_vend_sil = spark.table(f"{SILVER_DB}.vendedores")

# Pedidos com customer_code sem cliente cadastrado
ped_sem_cliente = df_ped_sil.join(
    df_cli_sil.select("customer_id"), df_ped_sil.customer_code == df_cli_sil.customer_id, "left_anti"
)
print(f"Pedidos com customer_code não encontrado em clientes: {ped_sem_cliente.count()}")

# Pedidos com seller_id sem vendedor cadastrado
ped_sem_vendedor = df_ped_sil.join(
    df_vend_sil.select("seller_id"), "seller_id", "left_anti"
)
print(f"Pedidos com seller_id não encontrado em vendedores: {ped_sem_vendedor.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC **Resultado das validações referenciadas acima estará em log no notebook de qualidade (04).**
