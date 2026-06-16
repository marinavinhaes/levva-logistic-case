# Resumo Executivo Técnico
## Case Data Engineer – Pipeline de Dados Analíticos

---

## O que foi construído

Pipeline completo de engenharia de dados em arquitetura **medallion (Bronze → Silver → Gold)**, transformando 9 fontes brutas heterogêneas em 10 tabelas Delta otimizadas para consumo por BI.

**Escopo das fontes:**
- 9 arquivos em 5 formatos (CSV com delimitadores distintos, JSON array, NDJSON, Excel .xlsx, TXT pipe-delimited)
- ~2.270 registros brutos distribuídos entre pedidos, itens, clientes, produtos, entregas, ocorrências, canais, regiões e vendedores

**Resultado entregue:**
- 6 tabelas dimensão + 4 tabelas fato na camada Gold
- Modelo pronto para conectar diretamente ao Power BI, Tableau ou Metabase
- Todas as perguntas de negócio do escopo respondem com uma única query nas tabelas Gold

---

## Principais Decisões Técnicas

**1. Arquitetura Medallion**
Optei por três camadas explícitas (Bronze/Silver/Gold) em vez de transformar diretamente as fontes. Isso garante rastreabilidade completa: qualquer anomalia nos dados finais pode ser investigada voltando à camada imediatamente anterior.

**2. Delta Tables em todas as camadas**
O formato Delta permite time-travel, schema enforcement e operações ACID. Cada tabela pode ser re-gerada com `mode("overwrite")` de forma idempotente — útil para reprocessamento durante desenvolvimento.

**3. Parsing defensivo de datas**
As fontes usam três formatos de data distintos sem padrão consistente. A solução usa `coalesce(to_date(col, fmt1), to_date(col, fmt2), to_date(col, fmt3))`, tentando cada formato em cascata. Datas não parseáveis resultam em `null` e são registradas como anomalia — nunca descartadas silenciosamente.

**4. Deduplicação baseada em prioridade, não em descarte aleatório**
Duplicatas em vendedores (V004, V008) e canais (CH05) foram tratadas com `row_number()` com ordenação que prioriza o registro mais confiável (canal válido, nome sem sufixo de teste), documentando a lógica de desempate.

**5. Fato enriquecida, não normalizada ao extremo**
A `fct_pedidos` carrega `channel_id` e `regional_code` diretamente (herdados do vendedor), evitando que o analista de BI precise fazer 3 joins para chegar à região de um pedido. Isso reduz complexidade das consultas sem comprometer o modelo.

---

## Principais Desafios Encontrados

| Desafio | Impacto | Como foi resolvido |
|---|---|---|
| 3 formatos de data distintos em todas as fontes | Joins por data falhavam silenciosamente | Parsing multi-formato em cascata com `coalesce` |
| Decimais com vírgula em `unit_price` (31 ocorrências) | Cast direto resultava em `null` | `regexp_replace(",", ".")` antes do cast |
| IDs em lowercase (`o00177`, `ch07`) | Joins sem retorno para esses registros | `upper(trim(...))` em todos os IDs antes de qualquer join |
| Duplicatas com conflito de dados (V004, CH05) | Impossível deduplicar por `distinct()` | `row_number()` com ordenação por confiabilidade |
| 49 itens de pedido sem cabeçalho correspondente | Potencial perda de dados ou erro de join | Preservados com flag `sem_cabecalho` na Silver |
| Estados por extenso vs sigla nos clientes | Joins por estado falhavam | Mapeamento normalizado para sigla UF |

---

## Visão Geral do Modelo Final

```
                     dim_calendario
                           │
dim_clientes ──────────────┤
dim_vendedores ────────────┤
dim_canais ────────────────┼──── fct_pedidos ──── fct_pedidos_itens ◄── dim_produtos
dim_regioes ───────────────┤          │
                           │          ├──── fct_entregas
                           │          └──── fct_ocorrencias
```

**Capacidades analíticas habilitadas:**
- Receita líquida por período, região, canal e categoria de produto
- Ticket médio e evolução temporal
- Taxa de cancelamento por canal, região e vendedor
- Taxa de atraso em entregas por transportadora, modal e região
- Volume e severidade de ocorrências de atendimento por tipo e canal
- Análise de clientes por segmento, porte e estado

---

## Próximos Passos Recomendados

**Curto prazo (sprint 1):**
- Conectar as tabelas Gold ao BI e validar os KPIs com os times de Operações, Comercial e Atendimento
- Implementar testes de qualidade automatizados (ex: Great Expectations) para rodar a cada ingestão
- Resolver os ~30% de clientes sem segmento junto à equipe de CRM (enriquecimento ou regra de negócio)

**Médio prazo (sprint 2-3):**
- Migrar para carga incremental com `MERGE INTO` substituindo o `overwrite` atual
- Implementar jobs agendados no Databricks Workflows para execução diária
- Criar tabela de agregação mensal (`agg_pedidos_mensal`) para acelerar dashboards executivos

**Longo prazo:**
- Migrar para Unity Catalog para governança de dados, controle de acesso por coluna e lineage automático
- Avaliar a inclusão do histórico de status de pedido (captura de eventos de mudança de estado) para análises de jornada
- Conectar dados de custo de entrega por item quando o ERP evoluir para esta granularidade
