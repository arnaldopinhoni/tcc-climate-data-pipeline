-- Tabela 8. Distribuicao dos dias por faixa de evapotranspiracao de referencia.
--
-- Base deduplicada dentro da janela de coleta: cada data-alvo conta uma vez, 25 datas
-- por municipio. Na versao anterior a contagem era feita sobre as 119 agregacoes da
-- camada historica, entao datas previstas por mais rodadas pesavam mais na distribuicao
-- do que datas previstas por poucas, sem qualquer razao meteorologica para isso.
--
-- Faixas conforme Garbanzo Leon et al. (2025).

with dedup as (
    {{ gold_dedup_janela() }}
)

select
    city,
    count(*) as dias,
    count(*) filter (where total_et0_fao_evapotranspiration < 3) as eto_baixa,
    count(*) filter (where total_et0_fao_evapotranspiration >= 3
                       and total_et0_fao_evapotranspiration < 5) as eto_moderada,
    count(*) filter (where total_et0_fao_evapotranspiration >= 5
                       and total_et0_fao_evapotranspiration < 7) as eto_alta,
    count(*) filter (where total_et0_fao_evapotranspiration >= 7) as eto_muito_alta,
    round(100.0 * count(*) filter (where total_et0_fao_evapotranspiration >= 3
                                     and total_et0_fao_evapotranspiration < 5)
          / count(*), 1) as pct_moderada
from dedup
group by 1
order by 1
