-- Horizonte de previsao por rodada e cobertura temporal acumulada.
--
-- Esclarece a diferenca entre o horizonte de 7 dias da camada corrente e a janela de
-- 25 dias observada na camada historica.
--
-- Toda rodada devolve exatamente 7 dias-alvo, com antecedencia de 0 a 6 dias: cada
-- valor de antecedencia tem 85 linhas na janela (17 rodadas x 5 cidades), sem excecao.
-- O horizonte nunca foi maior. O que cresce e a uniao das janelas de 17 rodadas
-- consecutivas, que cobre 25 datas de calendario, e o numero de rodadas por data-alvo,
-- que vai de 1 a 8.

with historico as (
    select *
    from {{ ref('gold_daily_summary_history') }}
    where ingestion_time::date between date '2026-03-14' and date '2026-04-01'
),

horizonte_por_rodada as (
    select
        'horizonte por rodada' as recorte,
        (day - ingestion_time::date)::text as chave,
        count(*) as linhas
    from historico
    group by 1, 2
),

rodadas_por_data_alvo as (
    select
        'rodadas por data-alvo' as recorte,
        day::text as chave,
        count(*) / count(distinct city) as linhas
    from historico
    group by 1, 2
)

select * from horizonte_por_rodada
union all
select * from rodadas_por_data_alvo
order by recorte, chave
