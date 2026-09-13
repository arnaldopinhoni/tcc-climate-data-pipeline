-- Cadencia de ingestao na janela do estudo.
--
-- Responde a duas perguntas: quantas rodadas houve de fato, e por que um pipeline
-- agendado como diario nao produziu uma rodada por dia.
--
-- Resultado na janela 14/03 a 01/04/2026: 17 rodadas fisicas distribuidas em 14 dias
-- distintos. Cinco dias sem nenhuma execucao (15, 22, 24, 26 e 30 de marco) e um dia
-- com quatro execucoes (31/03). O agendamento usa catchup=False, entao dia sem
-- execucao nao e recuperado depois.

with calendario as (
    select generate_series(
        date '2026-03-14',
        date '2026-04-01',
        interval '1 day'
    )::date as dia
),

rodadas as (
    select
        ingestion_time::date as dia,
        count(distinct ingestion_time) as rodadas,
        count(*) as linhas_bronze,
        string_agg(distinct to_char(ingestion_time, 'HH24:MI'), ', ' order by to_char(ingestion_time, 'HH24:MI')) as horarios
    from {{ source('raw', 'bronze_climate_raw') }}
    where ingestion_time::date between date '2026-03-14' and date '2026-04-01'
    group by 1
)

select
    calendario.dia,
    coalesce(rodadas.rodadas, 0) as rodadas,
    coalesce(rodadas.linhas_bronze, 0) as linhas_bronze,
    rodadas.horarios,
    case
        when rodadas.rodadas is null then 'sem execucao'
        when rodadas.rodadas > 1 then 'reexecucao no mesmo dia'
        else 'execucao unica'
    end as situacao
from calendario
left join rodadas on rodadas.dia = calendario.dia
order by calendario.dia
