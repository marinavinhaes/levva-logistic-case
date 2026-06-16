# Databricks notebook source
# MAGIC %md
# MAGIC # 00 – Exploração das Fontes de Dados
# MAGIC
# MAGIC **Objetivo:** Perfilar cada fonte bruta, identificar formatos, relacionamentos e problemas de qualidade antes de iniciar as transformações.
# MAGIC
# MAGIC | Fonte | Formato | Delimitador | Registros |
# MAGIC |---|---|---|---|
# MAGIC | erp_pedidos_cabecalho_2025.csv | CSV | `;` | 403 |
# MAGIC | erp_pedidos_itens_2025.csv | CSV | `,` | 995 |
# MAGIC | cadastro_produtos_api_dump.json | JSON array | – | 72 |
# MAGIC | comercial_canais.xlsx | Excel | – | 8 (com dups) |
# MAGIC | crm_clientes_export.xlsx | Excel | – | 183 |
# MAGIC | legado_regioes_pipe.txt | TXT | `\|` | 7 (com dups) |
# MAGIC | logistica_entregas.json | JSON array | – | 322 |
# MAGIC | atendimento_ocorrencias.ndjson | NDJSON | – | 270 |
# MAGIC | vendedores.csv | CSV | `;` | 42 (com dups) |

# COMMAND ----------

# MAGIC %md ## Configuração de caminhos

# COMMAND ----------

# Ajuste o caminho conforme o DBFS ou volume montado no seu Databricks
SOURCES_PATH = "/FileStore/sources"  # Altere conforme necessário

# COMMAND ----------

# MAGIC %md ## 1. erp_pedidos_cabecalho_2025.csv

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *

df_ped = (
    spark.read
    .option("header", "true")
    .option("sep", ";")
    .option("multiLine", "true")
    .option("escape", '"')
    .csv(f"{SOURCES_PATH}/erp_pedidos_cabecalho_2025.csv")
)

print(f"Linhas: {df_ped.count()}")
df_ped.printSchema()
df_ped.show(5, truncate=False)

# COMMAND ----------

# Distribuição de status_order
print("=== Valores de status_order ===")
df_ped.groupBy("status_order").count().orderBy("count", ascending=False).show()

# COMMAND ----------

# Verificar formatos de data inconsistentes
print("=== Amostras de order_date e promised_date ===")
df_ped.select("order_id", "order_date", "promised_date").sample(0.1, seed=42).show(20)

# COMMAND ----------

# Pedidos sem itens (verificação cruzada com pedidos_itens)
df_itens = (
    spark.read
    .option("header", "true")
    .option("sep", ",")
    .csv(f"{SOURCES_PATH}/erp_pedidos_itens_2025.csv")
)

orders_header = df_ped.select("order_id")
orders_items = df_itens.select(F.upper(F.trim(F.col("order_id"))).alias("order_id")).distinct()

orphans_header = orders_header.join(orders_items, "order_id", "left_anti")
orphans_items = orders_items.join(orders_header, "order_id", "left_anti")

print(f"Pedidos no cabeçalho sem itens: {orphans_header.count()}")
print(f"Pedidos nos itens sem cabeçalho: {orphans_items.count()}")
orphans_header.show()
orphans_items.show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Problemas identificados – pedidos_cabecalho:**
# MAGIC - `order_date` e `promised_date` com 3 formatos distintos: `YYYY-MM-DD`, `YYYY/MM/DD`, `DD/MM/YYYY`
# MAGIC - `status_order` com inconsistências de case (`Faturado` vs `faturado`, `EM_SEPARACAO` vs `em separacao`) e 1 registro vazio
# MAGIC - 18 pedidos no cabeçalho sem itens correspondentes → tratados como pedidos sem produto (ex: serviços)
# MAGIC - 49 linhas nos itens sem cabeçalho; parte tem `order_id` em lowercase (`o00177`) — corrigível por uppercase

# COMMAND ----------

# MAGIC %md ## 2. erp_pedidos_itens_2025.csv

# COMMAND ----------

df_itens.printSchema()
df_itens.show(5)

# COMMAND ----------

# item_status
print("=== Valores de item_status ===")
df_itens.groupBy("item_status").count().show()

# COMMAND ----------

# Detectar decimais com vírgula
from pyspark.sql.functions import col, regexp_extract

df_comma = df_itens.filter(
    col("unit_price").contains(",") | col("total_item").contains(",")
)
print(f"Linhas com vírgula como separador decimal: {df_comma.count()}")
df_comma.select("order_id", "item_seq", "unit_price", "total_item").show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC **Problemas identificados – pedidos_itens:**
# MAGIC - 31 linhas com vírgula como separador decimal em `unit_price` → substituir `,` por `.`
# MAGIC - `item_status` com case inconsistente (`Ativo` vs `ativo`) e campos vazios
# MAGIC - 49 orders sem cabeçalho, sendo alguns com `order_id` em lowercase (recuperáveis)

# COMMAND ----------

# MAGIC %md ## 3. cadastro_produtos_api_dump.json

# COMMAND ----------

df_prod_raw = spark.read.option("multiLine", "true").json(f"{SOURCES_PATH}/cadastro_produtos_api_dump.json")
df_prod_raw.printSchema()
df_prod_raw.show(3, truncate=False)

# COMMAND ----------

print("=== Status de produto ===")
df_prod_raw.groupBy("product.status").count().show()

print("=== Categorias ===")
df_prod_raw.groupBy("product.category").count().show()

print("=== Nulos em list_price ===")
df_prod_raw.filter(F.col("pricing.list_price").isNull()).count()

# COMMAND ----------

# MAGIC %md
# MAGIC **Problemas identificados – produtos:**
# MAGIC - `product.status` com case inconsistente (`Ativo` vs `ativo`) e valores `None`
# MAGIC - Estrutura aninhada (`product`, `pricing`, `attributes`) precisa ser achatada
# MAGIC - `attributes.tags` é array → será convertido para string concatenada

# COMMAND ----------

# MAGIC %md ## 4. comercial_canais.xlsx

# COMMAND ----------

df_canais = (
    spark.read
    .format("com.crealytics.spark.excel")
    .option("header", "true")
    .option("dataAddress", "'canais'!A1")
    .load(f"{SOURCES_PATH}/comercial_canais.xlsx")
)
df_canais.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC **Problemas identificados – canais:**
# MAGIC - `CH05` duplicado com nome diferente (`E-commerce` vs `ecommerce`) → manter primeira ocorrência e logar conflito
# MAGIC - `CH06` sem `nome_canal` → manter com flag de dado incompleto
# MAGIC - `ch07` com id em lowercase → normalizar para uppercase
# MAGIC - Campo `ativo` com case inconsistente (`sim`, `Sim`, `SIM`, `nao`)
# MAGIC - `CH04` marcado como inativo (`nao`) mas possui vendedores vinculados

# COMMAND ----------

# MAGIC %md ## 5. crm_clientes_export.xlsx

# COMMAND ----------

df_cli = (
    spark.read
    .format("com.crealytics.spark.excel")
    .option("header", "true")
    .load(f"{SOURCES_PATH}/crm_clientes_export.xlsx")
)
print(f"Linhas: {df_cli.count()}")
df_cli.printSchema()
df_cli.show(5, truncate=False)

# COMMAND ----------

# Nulos por coluna
from pyspark.sql.functions import count, when, isnan

print("=== Nulos por coluna ===")
df_cli.select([
    count(when(F.col(c).isNull() | (F.col(c) == ""), c)).alias(c)
    for c in df_cli.columns
]).show()

# COMMAND ----------

print("=== Variações de estado ===")
df_cli.groupBy("estado").count().orderBy("count", ascending=False).show(20)

# COMMAND ----------

# MAGIC %md
# MAGIC **Problemas identificados – clientes:**
# MAGIC - `data_cadastro` com formatos mistos: `YYYY-MM-DD`, `YYYY/MM/DD`
# MAGIC - `estado` mistura nome completo e sigla (`santa catarina`, `PR`, `Paraná`)
# MAGIC - `segmento` ausente em vários registros
# MAGIC - `status_cliente` ausente em vários registros → inferido como `Ativo` quando há pedidos recentes
# MAGIC - `porte` com case inconsistente (`Grande` vs `grande`)

# COMMAND ----------

# MAGIC %md ## 6. legado_regioes_pipe.txt

# COMMAND ----------

df_reg = (
    spark.read
    .option("header", "true")
    .option("sep", "|")
    .csv(f"{SOURCES_PATH}/legado_regioes_pipe.txt")
)
df_reg.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC **Problemas identificados – regiões:**
# MAGIC - `sul` é duplicata de `S` (mesma regional, id em lowercase)
# MAGIC - `SE` aparece duas vezes com estado diferente (`SP` vs `sao paulo`) → consolidar
# MAGIC - `XX` é registro inválido (`active_flag = 0`, sem estado e sem gestor) → excluir
# MAGIC - `manager_name` de `S` e `sul` é o mesmo gestor (`Rafael Souza`) — confirmação de duplicata

# COMMAND ----------

# MAGIC %md ## 7. logistica_entregas.json

# COMMAND ----------

df_ent_raw = spark.read.option("multiLine", "true").json(f"{SOURCES_PATH}/logistica_entregas.json")
df_ent_raw.printSchema()
df_ent_raw.show(3, truncate=False)

# COMMAND ----------

print("=== delivery_status ===")
df_ent_raw.groupBy("delivery_status").count().show()

print("=== carrier.mode ===")
df_ent_raw.groupBy("carrier.mode").count().show()

# COMMAND ----------

print(f"null delivery_status: {df_ent_raw.filter(F.col('delivery_status').isNull()).count()}")
print(f"null delivered_at:    {df_ent_raw.filter(F.col('timestamps.delivered_at').isNull()).count()}")
print(f"null carrier.name:    {df_ent_raw.filter(F.col('carrier.name').isNull()).count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC **Problemas identificados – entregas:**
# MAGIC - `delivery_status` com case inconsistente (`Delivered` vs `delivered`) e 60 nulos
# MAGIC - `carrier.mode` com inconsistência (`rodoviario` vs `Rodoviário`)
# MAGIC - 36 registros sem `delivered_at` — compatível com status `in_transit` ou `atrasado`
# MAGIC - 32 `order_ref` não encontrados no cabeçalho de pedidos → provavelmente de outros períodos; mantidos com flag

# COMMAND ----------

# MAGIC %md ## 8. atendimento_ocorrencias.ndjson

# COMMAND ----------

df_oc = spark.read.json(f"{SOURCES_PATH}/atendimento_ocorrencias.ndjson")
print(f"Linhas: {df_oc.count()}")
df_oc.printSchema()
df_oc.show(5, truncate=False)

# COMMAND ----------

print("=== event_type ===")
df_oc.groupBy("event_type").count().show()

print("=== status ===")
df_oc.groupBy("status").count().show()

print("=== severity ===")
df_oc.groupBy("severity").count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Problemas identificados – ocorrências:**
# MAGIC - `created_at` com 3 formatos: `YYYY-MM-DD HH:MM:SS`, `YYYY/MM/DD`, `DD/MM/YYYY HH:MM`
# MAGIC - `event_type` com case inconsistente (`Delay` vs `delay`) e nulos
# MAGIC - `status` com case inconsistente (`Open` vs `open`) e nulos
# MAGIC - `severity` com case inconsistente e nulos
# MAGIC - 27 `order_id` não encontrados no cabeçalho → mantidos com flag

# COMMAND ----------

# MAGIC %md ## 9. vendedores.csv

# COMMAND ----------

df_vend = (
    spark.read
    .option("header", "true")
    .option("sep", ";")
    .csv(f"{SOURCES_PATH}/vendedores.csv")
)
print(f"Linhas: {df_vend.count()}")
df_vend.show(truncate=False)

# COMMAND ----------

print("=== Duplicatas de seller_id ===")
df_vend.groupBy("seller_id").count().filter("count > 1").show()

print("=== Valores de status ===")
df_vend.groupBy("status").count().show()

print("=== Valores de regional_code ===")
df_vend.groupBy("regional_code").count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Problemas identificados – vendedores:**
# MAGIC - `V004` duplicado com `canal_id` diferente (`CH02` vs `CH99`) → `CH99` não existe nos canais; manter `CH02`
# MAGIC - `V008` duplicado com nome diferente (`Vendedor 8` vs `Vendedor 8 duplicado`) → manter primeira ocorrência
# MAGIC - `hire_date` com formatos mistos: `YYYY-MM-DD`, `DD/MM/YYYY`
# MAGIC - `regional_code` com `sul` (lowercase) → normalizar para `S`
# MAGIC - `canal_id` com `ch07` em lowercase → normalizar para `CH07`
# MAGIC - `status` com case inconsistente (`Ativo`, `ativo`, `inativo`, `Inativo`, vazio)
# MAGIC - `V006` com `hire_date = 29/02/2024` → válido (2024 é bissexto)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumo de Problemas de Qualidade
# MAGIC
# MAGIC | Fonte | Problema | Estratégia |
# MAGIC |---|---|---|
# MAGIC | pedidos_cabecalho | Formatos de data mistos | Parsing multi-formato com `coalesce(to_date(...))` |
# MAGIC | pedidos_cabecalho | `status_order` com case/espaço inconsistente | `upper + trim + replace` |
# MAGIC | pedidos_cabecalho | 18 pedidos sem itens | Manter; tratados como pedidos de serviço |
# MAGIC | pedidos_itens | Vírgula como decimal | `regexp_replace(',', '.')` antes do cast |
# MAGIC | pedidos_itens | `order_id` em lowercase | `upper(trim(order_id))` |
# MAGIC | pedidos_itens | 49 orders sem cabeçalho | Manter com flag `sem_cabecalho = true` |
# MAGIC | produtos | `status` com case inconsistente | `initcap` |
# MAGIC | canais | CH05 duplicado | Manter primeira ocorrência; logar conflito |
# MAGIC | canais | `ch07` em lowercase | `upper(trim(id_canal))` |
# MAGIC | clientes | `estado` com nome/sigla misto | Mapeamento para sigla padronizada |
# MAGIC | clientes | Formatos de data mistos | Parsing multi-formato |
# MAGIC | regiões | `sul` duplicata de `S` | Remover; normalizar referências para `S` |
# MAGIC | regiões | `XX` inválido | Excluir (active_flag = 0, sem dados) |
# MAGIC | entregas | `delivery_status` com case | `lower(trim(...))` |
# MAGIC | entregas | `carrier.mode` com case | `initcap` |
# MAGIC | ocorrências | Formatos de data mistos | Parsing multi-formato |
# MAGIC | ocorrências | `event_type`, `status`, `severity` com case | `lower(trim(...))` |
# MAGIC | vendedores | V004, V008 duplicados | Manter primeira ocorrência por seller_id |
# MAGIC | vendedores | `regional_code = sul` | Normalizar para `S` |
# MAGIC | vendedores | `canal_id` em lowercase | `upper(trim(...))` |
