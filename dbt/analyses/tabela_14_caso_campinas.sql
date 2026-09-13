-- Tabela 12. Evolucao das previsoes emitidas para 18 de marco de 2026 em Campinas.
--
-- Caso de maior divergencia de temperatura media na janela: 3,02 C entre as quatro
-- rodadas disponiveis. A coluna de antecedencia mostra a distancia entre a data da
-- rodada e a data prevista.
--
-- Observacao para o texto: a sequencia nao e de convergencia monotona. A temperatura
-- salta de 22,27 para 25,29 C e depois recua para 24,96 e 24,36 C. A precipitacao,
-- prevista como nula ate dois dias antes, aparece com 0,2 mm em D-1 e chega a 7,5 mm
-- em D-0, ou seja, a incerteza da chuva aumenta perto da data em vez de diminuir.

select
    row_number() over (order by ingestion_time) as rodada,
    to_char(ingestion_time, 'DD/MM/YYYY HH24"h"MI') as data_hora_ingestao,
    (day - ingestion_time::date) as dias_antecedencia,
    round(avg_temp, 2) as temp_media_prevista_c,
    round(min_temp, 2) as temp_min_prevista_c,
    round(max_temp, 2) as temp_max_prevista_c,
    round(total_precipitation, 1) as precip_prevista_mm
from {{ ref('gold_daily_summary_history') }}
where city = 'campinas'
  and day = date '2026-03-18'
  and ingestion_time::date between date '2026-03-14' and date '2026-04-01'
order by ingestion_time
