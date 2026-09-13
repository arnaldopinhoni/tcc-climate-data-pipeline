-- Tabela 3. Estatisticas descritivas das variaveis meteorologicas por municipio.
--
-- Base deduplicada dentro da janela de coleta: uma previsao por cidade e por data-alvo,
-- 25 datas (14/03 a 07/04), escolhida como a da rodada mais recente entre as 17 do
-- periodo. Ver a macro gold_dedup_janela para a ordem das operacoes.
--
-- As colunas de temperatura media, extremos e precipitacao vem do grao diario; o desvio
-- padrao e a umidade relativa vem do grao horario, porque a umidade nao e agregada na
-- camada gold e o desvio e calculado sobre as temperaturas horarias.
--
-- Versao anterior do trabalho somava a gold historica inteira, o que contava cada data
-- do calendario de 1 a 8 vezes e inflava a precipitacao do periodo em cerca de 5 vezes.

with diario as (
    {{ gold_dedup_janela() }}
),

horario as (
    {{ silver_dedup_janela() }}
),

por_cidade_diario as (
    select
        city,
        count(*) as dias,
        avg(avg_temp) as temp_media,
        min(min_temp) as temp_min,
        max(max_temp) as temp_max,
        sum(total_precipitation) as precipitacao_total
    from diario
    group by 1
),

por_cidade_horario as (
    select
        city,
        stddev_samp(temperature_2m) as desvio_padrao_temp,
        avg(relative_humidity_2m) as umidade_media
    from horario
    group by 1
)

select
    d.city,
    d.dias,
    round(d.temp_media, 2) as temp_media_c,
    round(d.temp_min, 2) as temp_min_c,
    round(d.temp_max, 2) as temp_max_c,
    round(h.desvio_padrao_temp, 2) as desvio_padrao_temp_c,
    round(d.precipitacao_total, 1) as precipitacao_total_mm,
    round(h.umidade_media, 1) as umidade_media_pct
from por_cidade_diario d
join por_cidade_horario h on h.city = d.city
order by d.city
