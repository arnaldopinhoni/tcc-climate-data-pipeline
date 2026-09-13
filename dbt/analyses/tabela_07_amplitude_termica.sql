-- Tabela 5. Amplitude termica diaria por municipio.
--
-- Amplitude = max_temp - min_temp DENTRO de uma mesma previsao, para cada data-alvo.
--
-- A versao anterior calculava max(max_temp) - min(min_temp) sobre TODAS as rodadas que
-- previram aquela data. Isso nao mede a oscilacao termica do dia: mede o envelope de
-- discordancia entre previsoes diferentes, que e sempre maior ou igual a amplitude real.
-- O efeito era de cerca de +1,8 C em todos os municipios.
--
-- A query traz as duas versoes lado a lado para deixar a diferenca explicita.

with dedup as (
    {{ gold_dedup_janela() }}
),

correto as (
    select
        city,
        avg(max_temp - min_temp) as media,
        min(max_temp - min_temp) as minima,
        max(max_temp - min_temp) as maxima
    from dedup
    group by 1
),

envelope_entre_rodadas as (
    select city, avg(amp) as media
    from (
        select city, day, max(max_temp) - min(min_temp) as amp
        from {{ ref('gold_daily_summary_history') }}
        where ingestion_time::date between date '2026-03-14' and date '2026-04-01'
        group by 1, 2
    ) por_dia
    group by 1
)

select
    c.city,
    round(c.media, 2) as amplitude_media_c,
    round(c.minima, 2) as amplitude_minima_c,
    round(c.maxima, 2) as amplitude_maxima_c,
    round(e.media, 2) as metodo_anterior_c,
    round(e.media - c.media, 2) as inflacao_c
from correto c
join envelope_entre_rodadas e on e.city = c.city
order by c.city
