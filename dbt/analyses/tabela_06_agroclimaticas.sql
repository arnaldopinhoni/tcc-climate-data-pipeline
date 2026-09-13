-- Tabela 4. Variaveis agroclimaticas por municipio.
--
-- Base deduplicada dentro da janela de coleta, 25 datas-alvo por cidade. A
-- evapotranspiracao acumulada aqui e um acumulado meteorologico legitimo: soma 25
-- valores diarios distintos, um por data.
--
-- Na versao anterior o mesmo campo somava 119 agregacoes por cidade, entre elas varias
-- previsoes para as mesmas datas, produzindo 478,9 a 490,6 mm em 25 dias, o que
-- equivaleria a cerca de 19 mm por dia e nao tem sentido fisico.

with dedup as (
    {{ gold_dedup_janela() }}
)

select
    city,
    count(*) as dias,
    round(avg(avg_dew_point_2m), 2) as ponto_orvalho_medio_c,
    round(avg(avg_shortwave_radiation), 1) as radiacao_media_wm2,
    round(avg(avg_wind_speed_10m), 2) as vento_medio_kmh,
    round(avg(avg_vapour_pressure_deficit), 3) as vpd_medio_kpa,
    round(sum(total_et0_fao_evapotranspiration), 1) as eto_total_mm,
    round(avg(total_et0_fao_evapotranspiration), 2) as eto_media_diaria_mm
from dedup
group by 1
order by 1
