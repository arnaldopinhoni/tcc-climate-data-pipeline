-- Tabela 6. Coeficientes de correlacao de Pearson entre variaveis meteorologicas.
--
-- Base deduplicada dentro da janela de coleta. Precipitacao x umidade e calculada no
-- grao horario; os demais pares no grao diario.
--
-- A correlacao entre radiacao e ETo nao e um achado empirico: a Open-Meteo calcula a
-- ETo pelo metodo FAO-56 a partir da propria radiacao, entre outras variaveis. O valor
-- alto e evidencia de consistencia interna do dado, nao de relacao descoberta.

with diario as (
    {{ gold_dedup_janela() }}
),

horario as (
    {{ silver_dedup_janela() }}
),

por_cidade_diario as (
    select
        city,
        corr(avg_temp, total_et0_fao_evapotranspiration) as temp_eto,
        corr(avg_shortwave_radiation, total_et0_fao_evapotranspiration) as radiacao_eto,
        count(*) as n_dias
    from diario
    group by 1
),

por_cidade_horario as (
    select
        city,
        corr(precipitation, relative_humidity_2m) as precipitacao_umidade,
        count(*) as n_horas
    from horario
    group by 1
)

select
    d.city,
    d.n_dias,
    h.n_horas,
    round(h.precipitacao_umidade::numeric, 3) as precipitacao_x_umidade,
    round(d.temp_eto::numeric, 3) as temperatura_x_eto,
    round(d.radiacao_eto::numeric, 3) as radiacao_x_eto
from por_cidade_diario d
join por_cidade_horario h on h.city = d.city
order by d.city
