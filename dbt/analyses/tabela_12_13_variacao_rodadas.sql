-- Tabelas 10 e 11. Divergencia entre rodadas de previsao para a mesma data-alvo.
--
-- Esta analise usa a camada HISTORICA de proposito: e exatamente para isso que ela
-- existe. Aqui a repeticao de datas nao e defeito, e o objeto de estudo.
--
-- Inclui um teste de robustez. Tres das 17 rodadas sao reexecucoes manuais do dia
-- 31/03 (13:26, 21:07, 21:09 e 21:10). Colapsando as reexecucoes do mesmo dia e
-- mantendo apenas a ultima, a variacao media muda de 1,37 / 1,33 / 1,18 / 0,91 / 1,01
-- para 1,33 / 1,30 / 1,17 / 0,89 / 1,00, e os maximos ficam identicos. Ou seja, o
-- achado nao e artefato das reexecucoes.

{% set janela %}
    ingestion_time::date between date '2026-03-14' and date '2026-04-01'
{% endset %}

with todas_rodadas as (
    select city, day,
           count(*) as rodadas,
           max(avg_temp) - min(avg_temp) as var_temp,
           max(total_precipitation) - min(total_precipitation) as var_precip
    from {{ ref('gold_daily_summary_history') }}
    where {{ janela }}
    group by 1, 2
    having count(*) > 1
),

uma_por_dia as (
    select city, day,
           count(*) as rodadas,
           max(avg_temp) - min(avg_temp) as var_temp,
           max(total_precipitation) - min(total_precipitation) as var_precip
    from (
        select distinct on (city, day, ingestion_time::date) *
        from {{ ref('gold_daily_summary_history') }}
        where {{ janela }}
        order by city, day, ingestion_time::date, ingestion_time desc, bronze_record_id desc
    ) colapsado
    group by 1, 2
    having count(*) > 1
)

select
    'todas as rodadas fisicas' as base,
    city,
    count(*) as datas_comparaveis,
    round(avg(var_temp), 2) as var_temp_media_c,
    round(max(var_temp), 2) as var_temp_maxima_c,
    round(avg(var_precip), 1) as var_precip_media_mm,
    round(max(var_precip), 1) as var_precip_maxima_mm
from todas_rodadas
group by 1, 2

union all

select
    'uma rodada por dia',
    city,
    count(*),
    round(avg(var_temp), 2),
    round(max(var_temp), 2),
    round(avg(var_precip), 1),
    round(max(var_precip), 1)
from uma_por_dia
group by 1, 2

order by base, city
