-- Tabela 7. Estrutura e volume da camada gold historica por municipio.
--
-- Esta tabela descreve a CAMADA, nao o clima do periodo. As 119 agregacoes por cidade
-- sao 17 rodadas x 7 dias-alvo, com sobreposicao entre rodadas: a mesma data aparece
-- de 1 a 8 vezes. As medias abaixo sao medias por agregacao, ponderadas pelo numero de
-- vezes que cada data foi prevista, e por isso nao devem ser lidas como medias
-- meteorologicas do periodo. Para isso, ver tabela_03 e tabela_04.

select
    city,
    count(*) as total_agregacoes,
    count(distinct bronze_record_id) as rodadas,
    count(distinct day) as datas_alvo_distintas,
    round(count(*)::numeric / count(distinct day), 2) as previsoes_por_data_alvo,
    round(avg(avg_temp), 2) as temp_media_por_agregacao_c,
    round(avg(max_temp), 2) as temp_max_media_por_agregacao_c,
    round(avg(total_precipitation), 1) as precip_media_por_agregacao_mm,
    round(avg(total_et0_fao_evapotranspiration), 2) as eto_media_por_agregacao_mm
from {{ ref('gold_daily_summary_history') }}
where ingestion_time::date between date '2026-03-14' and date '2026-04-01'
group by 1
order by 1
