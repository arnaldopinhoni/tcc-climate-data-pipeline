{#
    Bases deduplicadas restritas a uma janela de coleta.

    A ORDEM IMPORTA. O filtro por ingestion_time tem de ser aplicado ANTES da
    deduplicacao, nao depois.

    Os modelos gold_daily_summary_dedup e silver_climate_hourly_dedup deduplicam sobre
    todo o historico disponivel, o que e o comportamento correto em producao: para cada
    data-alvo eles entregam a melhor previsao ja emitida. Mas o banco segue recebendo
    rodadas depois do fim da coleta do estudo. Deduplicar primeiro e filtrar depois
    escolheria, para as datas de 02 a 07 de abril, rodadas de abril que estao fora da
    janela declarada no trabalho, e essas linhas seriam entao descartadas pelo filtro.

    Estas macros fazem na ordem certa: recortam a janela e so entao escolhem, para cada
    data-alvo, a rodada mais recente DENTRO da janela.

    Janela padrao: 14/03 a 01/04/2026, as 17 rodadas do periodo de monitoramento,
    cobrindo 25 datas-alvo de 14/03 a 07/04.
#}

{% macro gold_dedup_janela(inicio='2026-03-14', fim='2026-04-01') %}
    select distinct on (city, day)
        city,
        bronze_record_id,
        ingestion_time,
        day,
        (day - ingestion_time::date) as lead_days,
        avg_temp,
        max_temp,
        min_temp,
        total_precipitation,
        avg_dew_point_2m,
        avg_shortwave_radiation,
        avg_wind_speed_10m,
        avg_vapour_pressure_deficit,
        total_et0_fao_evapotranspiration
    from {{ ref('gold_daily_summary_history') }}
    where ingestion_time::date between date '{{ inicio }}' and date '{{ fim }}'
    order by city, day, ingestion_time desc, bronze_record_id desc
{% endmacro %}


{% macro silver_dedup_janela(inicio='2026-03-14', fim='2026-04-01') %}
    select distinct on (city, record_time)
        city,
        bronze_record_id,
        ingestion_time,
        record_time,
        (record_time::date - ingestion_time::date) as lead_days,
        temperature_2m,
        relative_humidity_2m,
        precipitation,
        dew_point_2m,
        shortwave_radiation,
        wind_speed_10m,
        vapour_pressure_deficit,
        et0_fao_evapotranspiration
    from {{ ref('silver_climate_hourly_history') }}
    where ingestion_time::date between date '{{ inicio }}' and date '{{ fim }}'
    order by city, record_time, ingestion_time desc, bronze_record_id desc
{% endmacro %}
