# Case Técnico – Engenheiro de Dados

## Visão Geral da Solução

Pipeline de dados em arquitetura medallion (Bronze → Silver → Gold) implementado em PySpark com Delta Tables no Databricks Community Edition.

**Fontes:** 9 arquivos em 5 formatos distintos (CSV, JSON, NDJSON, Excel, TXT pipe-delimited)  
**Destino:** 10 tabelas Delta na camada Gold (6 dimensões + 4 fatos) prontas para consumo por BI

---

## Estrutura do Repositório

```
.
├── notebooks/
│   ├── 00_exploracao_fontes.py   # Perfilamento de cada fonte
│   ├── 01_bronze_ingestao.py     # Ingestão bruta → Delta Bronze
│   ├── 02_silver_limpeza.py      # Limpeza e padronização → Delta Silver
│   └── 03_gold_modelagem.py      # Modelo analítico → Delta Gold
├── sources/                      # Arquivos brutos (não versionados)
│   ├── erp_pedidos_cabecalho_2025.csv
│   ├── erp_pedidos_itens_2025.csv
│   ├── cadastro_produtos_api_dump.json
│   ├── comercial_canais.xlsx
│   ├── crm_clientes_export.xlsx
│   ├── legado_regioes_pipe.txt
│   ├── logistica_entregas.json
│   ├── atendimento_ocorrencias.ndjson
│   └── vendedores.csv
└── docs/
    ├── README.md                 # Este arquivo
    └── resumo_executivo.md       # Resumo para liderança técnica
```

---

## Instruções de Execução

### Pré-requisitos

- Databricks Community Edition com cluster Spark 3.x
- Biblioteca `spark-excel` instalada no cluster:
  ```
  %pip install com.crealytics:spark-excel_2.12:0.14.0
  ```
- Arquivos de `sources/` carregados no DBFS:
  ```
  dbutils.fs.cp("file:/path/to/sources", "dbfs:/FileStore/sources", recurse=True)
  ```

### Ordem de execução

Execute os notebooks na sequência numérica:

1. `00_exploracao_fontes.py` — exploração e diagnóstico (opcional, mas recomendado)
2. `01_bronze_ingestao.py` — cria database `bronze` e 9 tabelas Delta
3. `02_silver_limpeza.py` — cria database `silver` e 9 tabelas limpas
4. `03_gold_modelagem.py` — cria database `gold` com 10 tabelas analíticas

Cada notebook é idempotente (`mode("overwrite")`): pode ser re-executado sem efeitos colaterais.

### Ajuste de caminhos

No início de cada notebook, ajuste as variáveis:

```python
SOURCES_PATH = "/FileStore/sources"   # onde estão os arquivos brutos
BRONZE_PATH  = "/delta/bronze"        # onde escrever os deltas
```

---

## Arquitetura da Solução

```
[Fontes Brutas]
      │
      ▼
 ┌─────────┐
 │  BRONZE │  Espelho fiel da fonte. Sem transformação de conteúdo.
 │         │  Cada tabela tem _source_file e _ingested_at.
 └────┬────┘
      │
      ▼
 ┌─────────┐
 │  SILVER │  Dados limpos, padronizados, deduplicados.
 │         │  Registros inválidos removidos ou sinalizados com flag.
 └────┬────┘
      │
      ▼
 ┌─────────┐
 │   GOLD  │  Modelo dimensional. Tabelas prontas para BI.
 │         │  Dimensões + Fatos. Métricas pré-calculadas.
 └─────────┘
```

---

## Problemas de Qualidade e Decisões de Tratamento

### 1. Formatos de data inconsistentes

**Fontes afetadas:** pedidos_cabecalho, pedidos_itens, clientes, ocorrencias, vendedores

As datas aparecem em três formatos: `yyyy-MM-dd`, `yyyy/MM/dd`, `dd/MM/yyyy`.

**Tratamento:** `coalesce(to_date(col, fmt1), to_date(col, fmt2), to_date(col, fmt3))`. Qualquer data não parseável resulta em `null` e é registrada como anomalia.

---

### 2. Inconsistências de case em campos categóricos

**Fontes afetadas:** todas

Exemplos: `Faturado` vs `faturado`, `Ativo` vs `ativo`, `sim` vs `Sim` vs `SIM`, `Open` vs `open`.

**Tratamento:** Normalização com `lower(trim(...))` ou `initcap(trim(...))` + mapeamento para valores canônicos. Cada campo tem sua estratégia documentada no notebook 02.

---

### 3. Duplicatas em chaves primárias

**Fonte:** vendedores.csv, comercial_canais.xlsx

- **V004:** dois registros com `canal_id` diferente (`CH02` e `CH99`). `CH99` não existe nos canais → mantida a linha com `CH02`.
- **V008:** dois registros com o mesmo `seller_id`, sendo um com sufixo "duplicado" no nome → mantida a primeira ocorrência (sem sufixo).
- **CH05:** dois registros com nome diferente (`E-commerce` vs `ecommerce`) → mantida a primeira ocorrência e conflito logado.

**Estratégia:** `row_number()` sobre a chave primária com ordenação que prioriza o registro mais confiável.

---

### 4. Decimais com vírgula como separador

**Fonte:** erp_pedidos_itens_2025.csv

31 linhas têm vírgula como separador decimal em `unit_price` (ex: `1274,78`).

**Tratamento:** `regexp_replace(col, ",", ".")` antes do `cast("double")`.

**Validação adicional:** calculado `total_item_calc = quantity * unit_price`; onde `|total_item - total_item_calc| > 0.01`, o valor calculado substitui o original e o campo `total_item_flag = 'DIVERGENCIA_TOTAL'` é setado.

---

### 5. IDs em lowercase

**Fontes:** pedidos_itens (order_id), vendedores (canal_id), canais (id_canal)

Exemplos: `o00177`, `ch07`, `CH05` vs `ch05`.

**Tratamento:** `upper(trim(col))` aplicado antes de qualquer join.

---

### 6. Duplicatas na tabela de regiões

**Fonte:** legado_regioes_pipe.txt

- `sul` e `S` representam a mesma regional → `sul` normalizado para `S`.
- `SE` aparece duas vezes com estados distintos (`SP` e `sao paulo`) → mantida a entrada com estado = `SP`.
- `XX` é um registro inválido (sem estado, sem gestor, `active_flag = 0`) → removido.

---

### 7. Registros órfãos (sem correspondência no cabeçalho de pedidos)

**Fontes afetadas:** pedidos_itens, entregas, ocorrencias

| Fonte | Orphans | Decisão |
|---|---|---|
| pedidos_itens | 49 linhas (parte com order_id em lowercase) | Após normalizar para uppercase, reduz. Restantes marcados com `sem_cabecalho = true` |
| entregas | 32 registros | Mantidos com flag; provavelmente pertencem a outros períodos |
| ocorrencias | 27 registros | Mantidos com flag; análises BI filtram por `order_sem_cabecalho = false` |

---

### 8. Campo `estado` em clientes com nome por extenso

**Fonte:** crm_clientes_export.xlsx

Mistura de sigla (`PR`) e nome completo (`santa catarina`, `Paraná`).

**Tratamento:** mapeamento para sigla UF de 2 letras.

---

## Modelo Analítico Gold

### Dimensões

| Tabela | Grão | Chave | Descrição |
|---|---|---|---|
| `dim_clientes` | 1 por cliente | `customer_id` | Cadastro de clientes com segmento, porte, localização |
| `dim_produtos` | 1 por produto | `product_id` | Catálogo com categoria, subcategoria, preço de lista |
| `dim_canais` | 1 por canal | `channel_id` | Canal de venda (Inside Sales, E-commerce, etc.) |
| `dim_regioes` | 1 por regional | `regional_code` | Regionais com estado e gestor responsável |
| `dim_vendedores` | 1 por vendedor | `seller_id` | Vendedor com canal e regional já enriquecidos |
| `dim_calendario` | 1 por dia | `date_key` | 2023–2026 com ano, mês, trimestre, semana, dia da semana |

### Fatos

| Tabela | Grão | Métricas principais |
|---|---|---|
| `fct_pedidos` | 1 por pedido | `gross_amount`, `discount_amount`, `net_amount`, `discount_pct`, `is_cancelled`, `is_delivered` |
| `fct_pedidos_itens` | 1 por item de pedido | `quantity`, `unit_price`, `total_item`, `unit_discount_vs_list_pct` |
| `fct_entregas` | 1 por entrega | `lead_time_days`, `is_delayed`, `delay_days`, `delivery_cost` |
| `fct_ocorrencias` | 1 por ocorrência | `event_type`, `severity`, `ticket_status` |

### Como responder as perguntas de negócio

| Pergunta | Tabelas necessárias |
|---|---|
| Receita total e ticket médio por período | `fct_pedidos` + `dim_calendario` |
| Desempenho por região | `fct_pedidos` + `dim_regioes` |
| Desempenho por canal | `fct_pedidos` + `dim_canais` |
| Receita por categoria de produto | `fct_pedidos_itens` + `dim_produtos` |
| Taxa de cancelamento | `fct_pedidos` (coluna `is_cancelled`) |
| Taxa de atraso nas entregas | `fct_entregas` (coluna `is_delayed`) |
| Ocorrências de atendimento | `fct_ocorrencias` |
| Análise de clientes (segmento, porte) | `fct_pedidos` + `dim_clientes` |

---

## Premissas Adotadas

1. **Período de análise:** Os dados de pedidos cobrem principalmente 2025, com algumas datas em 2026. A `dim_calendario` cobre 2023–2026 para acomodar datas de cadastro de clientes e vendedores.

2. **Pedidos sem itens:** 18 pedidos no cabeçalho não possuem itens correspondentes. Foram mantidos na `fct_pedidos` pois podem representar pedidos de serviços ou assinaturas sem itemização.

3. **Receita correta para itens com `total_item_flag = DIVERGENCIA_TOTAL`:** O valor calculado (`quantity × unit_price`) foi usado como fonte de verdade, assumindo que o erro está no campo `total_item` gravado pelo ERP.

4. **Status de pedido vazio:** Mapeado para `INDEFINIDO` em vez de descartado, para preservar os registros e permitir que o time de dados decida o tratamento correto.

5. **Vendedor com `canal_id` inválido (CH99):** O canal `CH99` não existe na tabela de canais. A segunda ocorrência de V004 (com `CH99`) foi descartada em favor da primeira (com `CH02`).

6. **Entregas e ocorrências órfãs:** Mantidas com flag `order_sem_cabecalho = true`. São excluídas das tabelas fato Gold (join com `fct_pedidos`) mas preservadas na Silver para auditoria.

---

## Limitações Conhecidas

- **Dados sem conexão entre `fct_entregas` e `fct_pedidos_itens`:** Não há `product_id` na tabela de entregas; análise de custo de frete por produto não é possível diretamente.
- **Clientes sem segmento:** ~30% dos clientes têm `segmento = Não Informado`. Análises por segmento terão cobertura parcial.
- **Histórico de status de pedido:** Apenas o status final está disponível; não é possível rastrear a jornada do pedido (ex: quando passou de `EM_SEPARACAO` para `FATURADO`).
- **Versão Community do Databricks:** Não suporta Unity Catalog, ACL nativo ou jobs agendados. Em produção, recomenda-se migrar para Workspace Edition com Unity Catalog para governança e agendamento.

---

## Sugestões de Evolução

1. **Adicionar coluna `ingestion_date` nas tabelas Gold** para suportar cargas incrementais com merge (`MERGE INTO`).
2. **Implementar testes de qualidade automatizados** com Great Expectations ou dbt tests antes da promoção Bronze → Silver.
3. **Enriquecer `dim_clientes` com dados de lifetime value** calculados a partir do histórico de `fct_pedidos`.
4. **Criar tabela agregada `agg_pedidos_mensal`** para acelerar dashboards de alto nível sem varrer a fato completa.
5. **Normalizar `fct_pedidos_itens` para incluir `order_date` como coluna de partição** para otimizar queries por período.
6. **Conectar `fct_entregas` à `fct_pedidos_itens`** via tabela intermediária de itens × entrega, quando a fonte de ERP evoluir.
