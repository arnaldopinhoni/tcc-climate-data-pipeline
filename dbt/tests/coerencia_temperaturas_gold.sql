-- Coerencia interna das temperaturas agregadas na camada gold:
-- a maxima nao pode ser menor que a media, e a media nao pode ser menor que a minima.
-- Cobre as quatro visoes gold de uma vez.

{% set gold_models = [
    'gold_daily_summary_history',
    'gold_daily_summary',
    'gold_daily_summary_dedup'
] %}

{% for model_name in gold_models %}
select
    '{{ model_name }}' as origem,
    city,
    day,
    avg_temp,
    max_temp,
    min_temp
from {{ ref(model_name) }}
where max_temp < avg_temp
   or avg_temp < min_temp
   or max_temp < min_temp
{% if not loop.last %}union all{% endif %}
{% endfor %}
