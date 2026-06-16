# Databricks notebook source
# MAGIC %md
# MAGIC # 03 – Gold: Modelagem Analítica
# MAGIC
# MAGIC **Objetivo:** Construir o modelo dimensional orientado ao consumo por BI.
# MAGIC
# MAGIC ## Modelo proposto
# MAGIC
# MAGIC ```
# MAGIC                          ┌──────────────────┐
# MAGIC                          │  dim_calendario  │
# MAGIC                          └────────┬─────────┘
# MAGIC                                   │ date_key
# MAGIC  ┌──────────────┐   ┌─────────────▼──────────────┐   ┌──────────────┐
# MAGIC  │ dim_clientes │◄──┤                            ├──►│dim_vendedores│
# MAGIC  └──────────────┘   │        fct_pedidos         │   └──────┬───────┘
# MAGIC  ┌──────────────┐   │  (1 linha por pedido)      │          │
# MAGIC  │  dim_canais  │◄──┤                            │   ┌──────▼───────┐
# MAGIC  └──────────────┘   └──────────┬─────────────────┘   │ dim_regioes  │
# MAGIC  ┌──────────────┐              │ order_id             └──────────────┘
# MAGIC  │  dim_produtos│◄──┐          │
# MAGIC  └──────────────┘   │ ┌────────▼──────────────┐
# MAGIC                     └─┤  fct_pedidos_itens    │
# MAGIC                       │  (1 linha por item)   │
# MAGIC                       └───────────────────────┘
# MAGIC                       ┌───────────────────────┐
# MAGIC                       │    fct_entregas       │  ◄── order_id → fct_pedidos
# MAGIC                       └───────────────────────┘
# MAGIC                       ┌───────────────────────┐
# MAGIC                       │   fct_ocorrencias     │  ◄── order_id → fct_pedidos
# MAGIC                       └───────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ### Granularidade
# MAGIC | Tabela | Grão |
# MAGIC |---|---|
# MAGIC | `dim_clientes` | 1 linha por cliente |
# MAGIC | `dim_produtos` | 1 linha por produto |
# MAGIC | `dim_canais` | 1 linha por canal de venda |
# MAGIC | `dim_regioes` | 1 linha por regional |
# MAGIC | `dim_vendedores` | 1 linha por vendedor |
# MAGIC | `dim_calendario` | 1 linha por dia |
# MAGIC | `fct_pedidos` | 1 linha por pedido |
# MAGIC | `fct_pedidos_itens` | 1 linha por item de pedido |
# MAGIC | `fct_entregas` | 1 linha por entrega |
# MAGIC | `fct_ocorrencias` | 1 linha por ocorrência de atendimento |

# COMMAND ----------

from pyspark.sql import functions as F, Window
from pyspark.sql.types import *
from datetime import date

SILVER_DB = "silver"
GOLD_DB   = "gold"
GOLD_PATH = "/delta/gold"

spark.sql(f"CREATE DATABASE IF NOT EXISTS {GOLD_DB}")

# COMMAND ----------

def write_gold(df, table_name: str, partition_cols: list = None):
    """Persiste um DataFrame como Delta Table na camada Gold."""
    path = f"{GOLD_PATH}/{table_name}"
    writer = (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option("path", path)
    )
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.saveAsTable(f"{GOLD_DB}.{table_name}")
    count = spark.table(f"{GOLD_DB}.{table_name}").count()
    print(f"[OK] {GOLD_DB}.{table_name} → {count} linhas")

# COMMAND ----------

# Carregar Silver
df_ped_cab  = spark.table(f"{SILVER_DB}.pedidos_cabecalho")
df_ped_it   = spark.table(f"{SILVER_DB}.pedidos_itens")
df_clientes = spark.table(f"{SILVER_DB}.clientes")
df_produtos = spark.table(f"{SILVER_DB}.produtos")
df_canais   = spark.table(f"{SILVER_DB}.canais")
df_regioes  = spark.table(f"{SILVER_DB}.regioes")
df_vend     = spark.table(f"{SILVER_DB}.vendedores")
df_entregas = spark.table(f"{SILVER_DB}.entregas")
df_oc       = spark.table(f"{SILVER_DB}.ocorrencias")

# COMMAND ----------

# MAGIC %md ## Dimensões

# COMMAND ----------

# MAGIC %md ### dim_clientes

# COMMAND ----------

dim_clientes = (
    df_clientes
    .select(
        F.col("customer_id"),
        F.col("nome_cliente").alias("customer_name"),
        F.col("segmento").alias("segment"),
        F.col("porte").alias("company_size"),
        F.col("cidade").alias("city"),
        F.col("estado").alias("state"),
        F.col("status_cliente").alias("customer_status"),
        F.col("data_cadastro").alias("registration_date"),
        F.col("email"),
        F.col("updated_at"),
    )
)

write_gold(dim_clientes, "dim_clientes")
dim_clientes.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md ### dim_produtos

# COMMAND ----------

dim_produtos = (
    df_produtos
    .select(
        F.col("product_id"),
        F.col("product_name"),
        F.col("category"),
        F.col("subcategory"),
        F.col("status").alias("product_status"),
        F.col("list_price"),
        F.col("currency"),
        F.col("family"),
        F.col("tags"),
        F.col("updated_at"),
    )
)

write_gold(dim_produtos, "dim_produtos")

# COMMAND ----------

# MAGIC %md ### dim_canais

# COMMAND ----------

dim_canais = (
    df_canais
    .select(
        F.col("id_canal").alias("channel_id"),
        F.col("nome_canal").alias("channel_name"),
        F.col("tipo_canal").alias("channel_type"),
        F.col("ativo").alias("is_active"),
    )
)

write_gold(dim_canais, "dim_canais")
dim_canais.show()

# COMMAND ----------

# MAGIC %md ### dim_regioes

# COMMAND ----------

dim_regioes = (
    df_regioes
    .select(
        F.col("regional_code"),
        F.col("regional_name"),
        F.col("state"),
        F.col("manager_name"),
        F.col("active_flag").cast("boolean").alias("is_active"),
    )
)

write_gold(dim_regioes, "dim_regioes")
dim_regioes.show()

# COMMAND ----------

# MAGIC %md ### dim_vendedores
# MAGIC
# MAGIC Enriquecida com informações de canal e região para facilitar análises sem joins adicionais.

# COMMAND ----------

dim_vendedores = (
    df_vend
    .join(df_canais.select("id_canal", "nome_canal", "tipo_canal"),
          df_vend.canal_id == df_canais.id_canal, "left")
    .join(df_regioes.select("regional_code", "regional_name", F.col("state").alias("regional_state")),
          "regional_code", "left")
    .select(
        F.col("seller_id"),
        F.col("seller_name"),
        F.col("canal_id").alias("channel_id"),
        F.col("nome_canal").alias("channel_name"),
        F.col("tipo_canal").alias("channel_type"),
        F.col("regional_code"),
        F.col("regional_name"),
        F.col("regional_state"),
        F.col("hire_date"),
        F.col("status").alias("seller_status"),
    )
)

write_gold(dim_vendedores, "dim_vendedores")
dim_vendedores.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md ### dim_calendario
# MAGIC
# MAGIC Cobre o período completo dos dados (2023–2026) para suportar análises temporais completas.

# COMMAND ----------

from pyspark.sql.functions import sequence, explode, to_date, lit, col
from pyspark.sql.types import DateType

# Gerar sequência de datas
df_cal = spark.range(1).select(
    explode(sequence(to_date(lit("2023-01-01")), to_date(lit("2026-12-31")))).alias("date_key")
)

dim_calendario = (
    df_cal
    .withColumn("year",          F.year("date_key"))
    .withColumn("quarter",       F.quarter("date_key"))
    .withColumn("month",         F.month("date_key"))
    .withColumn("month_name",    F.date_format("date_key", "MMMM"))
    .withColumn("week_of_year",  F.weekofyear("date_key"))
    .withColumn("day_of_month",  F.dayofmonth("date_key"))
    .withColumn("day_of_week",   F.dayofweek("date_key"))
    .withColumn("day_name",      F.date_format("date_key", "EEEE"))
    .withColumn("is_weekend",
        F.when(F.dayofweek("date_key").isin(1, 7), True).otherwise(False)
    )
    .withColumn("year_month",    F.date_format("date_key", "yyyy-MM"))
    .withColumn("year_quarter",
        F.concat(F.year("date_key"), F.lit("-Q"), F.quarter("date_key"))
    )
)

write_gold(dim_calendario, "dim_calendario")
print(f"dim_calendario: {dim_calendario.count()} dias")

# COMMAND ----------

# MAGIC %md ## Tabelas Fato

# COMMAND ----------

# MAGIC %md ### fct_pedidos
# MAGIC
# MAGIC **Grão:** 1 linha por pedido.
# MAGIC
# MAGIC Contém todas as chaves de dimensão e métricas de valor do pedido.
# MAGIC Joins com vendedores fornecem channel_id e regional_code diretamente,
# MAGIC eliminando a necessidade de joins encadeados nas consultas BI.

# COMMAND ----------

fct_pedidos = (
    df_ped_cab
    # Enriquecer com dados do vendedor (canal e região)
    .join(
        df_vend.select("seller_id", "canal_id", "regional_code"),
        "seller_id", "left"
    )
    .select(
        # Chaves
        F.col("order_id"),
        F.col("customer_code").alias("customer_id"),
        F.col("seller_id"),
        F.col("canal_id").alias("channel_id"),
        F.col("regional_code"),
        # Datas (FK para dim_calendario)
        F.col("order_date"),
        F.col("promised_date"),
        F.col("last_update"),
        # Status
        F.col("status_order"),
        # Flags derivados
        F.when(F.col("status_order") == "CANCELADO", True).otherwise(False).alias("is_cancelled"),
        F.when(F.col("status_order") == "ENTREGUE", True).otherwise(False).alias("is_delivered"),
        # Métricas financeiras
        F.col("gross_amount"),
        F.col("discount_amount"),
        F.col("net_amount"),
        F.round(
            F.when(F.col("gross_amount") > 0,
                   F.col("discount_amount") / F.col("gross_amount") * 100)
            .otherwise(F.lit(0.0)), 2
        ).alias("discount_pct"),
        # Detalhes de pagamento
        F.col("payment_source"),
        F.col("payment_priority"),
    )
)

write_gold(fct_pedidos, "fct_pedidos", partition_cols=["status_order"])
fct_pedidos.show(5)

# COMMAND ----------

# Validações da fact
total = fct_pedidos.count()
cancelados = fct_pedidos.filter("is_cancelled").count()
entregues  = fct_pedidos.filter("is_delivered").count()
sem_cliente = fct_pedidos.filter(F.col("customer_id").isNull()).count()
sem_canal   = fct_pedidos.filter(F.col("channel_id").isNull()).count()
sem_regiao  = fct_pedidos.filter(F.col("regional_code").isNull()).count()
receita_total = fct_pedidos.filter(~F.col("is_cancelled")).agg(F.sum("net_amount")).collect()[0][0]

print(f"Total de pedidos:             {total:,}")
print(f"Pedidos cancelados:           {cancelados:,} ({cancelados/total*100:.1f}%)")
print(f"Pedidos entregues:            {entregues:,} ({entregues/total*100:.1f}%)")
print(f"Sem customer_id no CRM:       {sem_cliente:,}")
print(f"Sem channel_id:               {sem_canal:,}")
print(f"Sem regional_code:            {sem_regiao:,}")
print(f"Receita líquida (não canc.): R$ {receita_total:,.2f}")

# COMMAND ----------

# MAGIC %md ### fct_pedidos_itens
# MAGIC
# MAGIC **Grão:** 1 linha por item de pedido.
# MAGIC
# MAGIC Inclui dados do produto e do pedido pai para permitir análises diretas de
# MAGIC receita por produto, categoria e canal sem joins adicionais.

# COMMAND ----------

fct_pedidos_itens = (
    df_ped_it
    # Trazer dados do pedido pai
    .join(
        fct_pedidos.select(
            "order_id", "customer_id", "seller_id", "channel_id",
            "regional_code", "order_date", "status_order", "is_cancelled"
        ),
        "order_id", "left"
    )
    # Trazer dados do produto
    .join(
        dim_produtos.select("product_id", "product_name", "category", "subcategory", "family", "list_price"),
        df_ped_it.product_code == dim_produtos.product_id, "left"
    )
    .select(
        # Chaves
        F.col("order_id"),
        F.col("item_seq"),
        F.col("product_code").alias("product_id"),
        F.col("product_name"),
        F.col("category").alias("product_category"),
        F.col("subcategory").alias("product_subcategory"),
        F.col("family").alias("product_family"),
        # Contexto do pedido
        F.col("customer_id"),
        F.col("seller_id"),
        F.col("channel_id"),
        F.col("regional_code"),
        F.col("order_date"),
        F.col("status_order"),
        F.col("is_cancelled"),
        # Métricas do item
        F.col("quantity"),
        F.col("unit_price"),
        F.col("total_item"),
        F.col("list_price"),
        # Desconto por item (preço unitário vs preço de lista)
        F.round(
            F.when(
                (F.col("list_price").isNotNull()) & (F.col("list_price") > 0),
                (F.col("list_price") - F.col("unit_price")) / F.col("list_price") * 100
            ).otherwise(F.lit(None)), 2
        ).alias("unit_discount_vs_list_pct"),
        # Item flags
        F.col("item_status"),
        F.col("sem_cabecalho"),
        F.col("total_item_flag"),
    )
)

write_gold(fct_pedidos_itens, "fct_pedidos_itens", partition_cols=["product_category"])

print(f"Total de itens: {fct_pedidos_itens.count():,}")
fct_pedidos_itens.groupBy("product_category").agg(
    F.count("*").alias("qtd_linhas"),
    F.sum("total_item").alias("receita_total")
).orderBy("receita_total", ascending=False).show()

# COMMAND ----------

# MAGIC %md ### fct_entregas
# MAGIC
# MAGIC **Grão:** 1 linha por entrega.
# MAGIC
# MAGIC Inclui SLA calculado (dias entre promised_date e delivered_at) e flag de atraso.

# COMMAND ----------

fct_entregas = (
    df_entregas
    .filter(~F.col("order_sem_cabecalho"))  # Foco em entregas rastreáveis
    .join(
        fct_pedidos.select(
            "order_id", "customer_id", "seller_id", "channel_id",
            "regional_code", "order_date", "promised_date"
        ),
        df_entregas.order_ref == fct_pedidos.order_id, "left"
    )
    .select(
        F.col("delivery_id"),
        F.col("order_ref").alias("order_id"),
        F.col("customer_id"),
        F.col("channel_id"),
        F.col("regional_code"),
        F.col("carrier_name"),
        F.col("carrier_mode"),
        F.col("delivery_status"),
        F.col("shipped_at"),
        F.col("delivered_at"),
        F.col("order_date"),
        F.col("promised_date"),
        F.col("dest_state"),
        F.col("dest_city"),
        F.col("delivery_cost"),
        # SLA: dias entre pedido e entrega
        F.when(
            F.col("delivered_at").isNotNull(),
            F.datediff(F.to_date(F.col("delivered_at")), F.col("order_date"))
        ).alias("lead_time_days"),
        # Flag de atraso: entregue após a data prometida
        F.when(
            F.col("delivered_at").isNotNull() & F.col("promised_date").isNotNull(),
            F.to_date(F.col("delivered_at")) > F.col("promised_date")
        ).otherwise(
            # Status "atrasado" ou "in_transit" sem entrega também é atraso potencial
            F.when(F.col("delivery_status").isin("atrasado"), True).otherwise(None)
        ).alias("is_delayed"),
        # Dias de atraso (positivo = atrasado, negativo = adiantado)
        F.when(
            F.col("delivered_at").isNotNull() & F.col("promised_date").isNotNull(),
            F.datediff(F.to_date(F.col("delivered_at")), F.col("promised_date"))
        ).alias("delay_days"),
        F.col("order_sem_cabecalho"),
    )
)

write_gold(fct_entregas, "fct_entregas")

# KPIs de entrega
total_ent = fct_entregas.count()
atrasadas = fct_entregas.filter(F.col("is_delayed") == True).count()
print(f"Total entregas (rastreáveis): {total_ent:,}")
print(f"Entregas com atraso:          {atrasadas:,} ({atrasadas/total_ent*100:.1f}%)")
fct_entregas.groupBy("delivery_status").count().show()

# COMMAND ----------

# MAGIC %md ### fct_ocorrencias
# MAGIC
# MAGIC **Grão:** 1 linha por ocorrência de atendimento.

# COMMAND ----------

fct_ocorrencias = (
    df_oc
    .filter(~F.col("order_sem_cabecalho"))
    .join(
        fct_pedidos.select(
            "order_id", "customer_id", "seller_id", "channel_id",
            "regional_code", "order_date", "status_order"
        ),
        "order_id", "left"
    )
    .select(
        F.col("ticket_id"),
        F.col("order_id"),
        F.col("customer_id"),
        F.col("channel_id"),
        F.col("regional_code"),
        F.col("order_date"),
        F.col("created_at").alias("ticket_created_at"),
        F.to_date(F.col("created_at")).alias("ticket_date"),
        F.col("event_type"),
        F.col("severity"),
        F.col("status").alias("ticket_status"),
        F.col("status_order").alias("order_status_at_ticket"),
        F.col("order_sem_cabecalho"),
    )
)

write_gold(fct_ocorrencias, "fct_ocorrencias")

print(f"Total de ocorrências (rastreáveis): {fct_ocorrencias.count():,}")
fct_ocorrencias.groupBy("event_type").count().orderBy("count", ascending=False).show()

# COMMAND ----------

# MAGIC %md ## Inventário da Camada Gold

# COMMAND ----------

tabelas_gold = [
    "dim_clientes", "dim_produtos", "dim_canais",
    "dim_regioes", "dim_vendedores", "dim_calendario",
    "fct_pedidos", "fct_pedidos_itens", "fct_entregas", "fct_ocorrencias"
]

print(f"{'Tabela':<25} {'Linhas':>8} {'Colunas':>8}")
print("-" * 45)
for t in tabelas_gold:
    df = spark.table(f"{GOLD_DB}.{t}")
    print(f"{t:<25} {df.count():>8} {len(df.columns):>8}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Consultas de Validação Analítica
# MAGIC
# MAGIC Exemplos do tipo de análise que o Analista de BI conseguirá fazer diretamente nas tabelas Gold.

# COMMAND ----------

# MAGIC %md ### Receita líquida por mês e canal

# COMMAND ----------

spark.sql("""
    SELECT
        DATE_FORMAT(p.order_date, 'yyyy-MM') AS ano_mes,
        c.channel_name,
        COUNT(*)               AS qtd_pedidos,
        SUM(p.net_amount)      AS receita_liquida,
        AVG(p.net_amount)      AS ticket_medio,
        SUM(CASE WHEN p.is_cancelled THEN 1 ELSE 0 END) / COUNT(*) * 100 AS taxa_cancelamento_pct
    FROM gold.fct_pedidos p
    LEFT JOIN gold.dim_canais c ON p.channel_id = c.channel_id
    GROUP BY 1, 2
    ORDER BY 1, 3 DESC
""").show(20)

# COMMAND ----------

# MAGIC %md ### Receita por região e categoria de produto

# COMMAND ----------

spark.sql("""
    SELECT
        r.regional_name,
        i.product_category,
        COUNT(DISTINCT i.order_id)   AS qtd_pedidos,
        SUM(i.total_item)            AS receita_bruta,
        ROUND(AVG(i.unit_discount_vs_list_pct), 2) AS desconto_medio_pct
    FROM gold.fct_pedidos_itens i
    LEFT JOIN gold.dim_regioes r ON i.regional_code = r.regional_code
    WHERE NOT i.is_cancelled
    GROUP BY 1, 2
    ORDER BY 4 DESC
""").show(20)

# COMMAND ----------

# MAGIC %md ### Taxa de atraso por transportadora e modal

# COMMAND ----------

spark.sql("""
    SELECT
        carrier_mode,
        carrier_name,
        COUNT(*)   AS total_entregas,
        SUM(CASE WHEN is_delayed THEN 1 ELSE 0 END) AS atrasadas,
        ROUND(SUM(CASE WHEN is_delayed THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) AS taxa_atraso_pct,
        ROUND(AVG(lead_time_days), 1) AS lead_time_medio_dias,
        ROUND(AVG(delay_days), 1)     AS atraso_medio_dias
    FROM gold.fct_entregas
    WHERE delivery_status = 'delivered'
    GROUP BY 1, 2
    ORDER BY 5 DESC
""").show()

# COMMAND ----------

# MAGIC %md ### Ocorrências por tipo e severidade

# COMMAND ----------

spark.sql("""
    SELECT
        event_type,
        severity,
        ticket_status,
        COUNT(*) AS qtd,
        COUNT(DISTINCT order_id) AS pedidos_afetados
    FROM gold.fct_ocorrencias
    GROUP BY 1, 2, 3
    ORDER BY 4 DESC
""").show(20)

# COMMAND ----------

# MAGIC %md ### Top 10 clientes por receita (não cancelados)

# COMMAND ----------

spark.sql("""
    SELECT
        p.customer_id,
        c.customer_name,
        c.segment,
        c.company_size,
        c.state,
        COUNT(*)          AS qtd_pedidos,
        SUM(p.net_amount) AS receita_total,
        AVG(p.net_amount) AS ticket_medio
    FROM gold.fct_pedidos p
    LEFT JOIN gold.dim_clientes c ON p.customer_id = c.customer_id
    WHERE NOT p.is_cancelled
    GROUP BY 1, 2, 3, 4, 5
    ORDER BY 7 DESC
    LIMIT 10
""").show(truncate=False)
