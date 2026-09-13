# analyses/

Queries que reproduzem cada tabela e cada numero apresentado no TCC.

Rodar com:

```bash
uv run dbt compile --project-dir dbt --target dev --select path:analyses
```

O SQL compilado sai em `dbt/target/compiled/clima/analyses/` e pode ser executado
diretamente no PostgreSQL.

## Janela do estudo

Todas as queries filtram a janela de monitoramento do trabalho:

```sql
ingestion_time::date between date '2026-03-14' and date '2026-04-01'
```

Sao 19 dias de calendario, com 17 rodadas de ingestao distribuidas em 14 dias
distintos. Ver `cadencia_ingestao.sql`.

## Qual base usar

| Finalidade | Modelo |
|---|---|
| Caracterizar o periodo meteorologico | `gold_daily_summary_dedup` / `silver_climate_hourly_dedup` |
| Medir divergencia entre rodadas de previsao | `gold_daily_summary_history` |
| Consumo operacional da previsao mais recente | `gold_daily_summary` |

A camada `*_history` guarda uma linha por (cidade, rodada, data-alvo). Somar
precipitacao ou evapotranspiracao sobre ela nao produz o acumulado do periodo,
e sim a soma de previsoes repetidas para as mesmas datas.

## Indice

A numeracao dos arquivos segue a numeracao das tabelas no trabalho revisado.

| Arquivo | Produz |
|---|---|
| `tabela_02_volume_por_camada.sql` | Tabela 2, volume de registros por camada |
| `tabela_03_cadencia_ingestao.sql` | Tabela 3, calendario das rodadas, dias sem execucao e reexecucoes |
| `tabela_04_horizonte_cobertura.sql` | Tabela 4, horizonte por rodada e rodadas por data-alvo |
| `tabela_05_caracterizacao.sql` | Tabela 5, estatisticas descritivas por municipio |
| `tabela_06_agroclimaticas.sql` | Tabela 6, variaveis agroclimaticas por municipio |
| `tabela_07_amplitude_termica.sql` | Tabela 7, amplitude termica diaria, com o metodo anterior lado a lado |
| `tabela_08_correlacoes.sql` | Tabela 8, correlacoes de Pearson entre variaveis |
| `tabela_09_camada_gold_historica.sql` | Tabela 9, estrutura e volume da camada gold historica |
| `tabela_10_faixas_eto.sql` | Tabela 10, distribuicao das datas por faixa de ETo |
| `tabela_11_dias_chuva.sql` | Tabela 11, datas com precipitacao prevista acima de 5 mm |
| `tabela_12_13_variacao_rodadas.sql` | Tabelas 12 e 13, divergencia entre rodadas, com teste de robustez |
| `tabela_14_caso_campinas.sql` | Tabela 14, evolucao das previsoes para 18/03 em Campinas |
| `metricas_operacionais.sql` | Payload e throughput da ingestao (nao consta do texto) |

A Tabela 15, validacao contra a reanalise ERA5, nao esta aqui porque depende de chamada
externa: ver `etl/validation/validate_against_era5.py`.
