-- Metricas operacionais da ingestao, medidas a partir da propria camada bronze.
--
-- Complementa as metricas de orquestracao lidas da metadata do Airflow
-- (etl/metrics/airflow_metrics.py), que trazem duracao por tarefa e taxa de sucesso.
--
-- Aqui saem volume de payload e throughput de registros. O payload e medido com
-- pg_column_size sobre o JSONB, ja comprimido pelo TOAST, e com octet_length sobre a
-- representacao textual, que e o tamanho trafegado da resposta da API.
--
-- Nao ha metrica de latencia de gravacao: ingestion_time e created_at recebem ambos o
-- NOW() da mesma transacao, entao a diferenca entre eles e sempre zero e nao mede nada.

with por_registro as (
    select
        ingestion_time,
        city,
        pg_column_size(raw_json) as bytes_armazenados,
        octet_length(raw_json::text) as bytes_texto,
        jsonb_array_length(raw_json -> 'hourly' -> 'time') as valores_horarios
    from {{ source('raw', 'bronze_climate_raw') }}
    where ingestion_time::date between date '2026-03-14' and date '2026-04-01'
)

select
    count(*) as respostas,
    count(distinct ingestion_time) as rodadas,
    count(distinct city) as cidades,
    sum(valores_horarios) as registros_horarios_gerados,
    round(avg(bytes_armazenados)) as payload_medio_bytes,
    round(min(bytes_armazenados)) as payload_min_bytes,
    round(max(bytes_armazenados)) as payload_max_bytes,
    round(avg(bytes_texto)) as payload_texto_medio_bytes,
    pg_size_pretty(sum(bytes_armazenados)::bigint) as volume_bronze_total
from por_registro
