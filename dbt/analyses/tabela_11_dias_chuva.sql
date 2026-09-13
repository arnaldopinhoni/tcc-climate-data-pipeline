-- Tabela 9. Dias com precipitacao prevista superior a 5 mm por municipio.
--
-- Base deduplicada dentro da janela de coleta: contagem de DATAS do calendario, nao de
-- agregacoes. Na versao anterior, uma data prevista como chuvosa por seis rodadas era
-- contada seis vezes, o que fazia a frequencia de dias chuvosos parecer maior do que
-- foi de fato previsto.

with dedup as (
    {{ gold_dedup_janela() }}
)

select
    city,
    count(*) as dias_avaliados,
    count(*) filter (where total_precipitation > 5) as dias_chuva_significativa,
    round(avg(total_precipitation) filter (where total_precipitation > 5), 1) as media_nesses_dias_mm,
    round(max(total_precipitation), 1) as maior_precipitacao_diaria_mm
from dedup
group by 1
order by 1
