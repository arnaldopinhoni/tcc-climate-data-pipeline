-- Tabela de ingestao bruta (Bronze Layer) para multiplas cidades
CREATE TABLE IF NOT EXISTS public.bronze_climate_raw (
    id SERIAL PRIMARY KEY,
    city TEXT NOT NULL,
    raw_json JSONB NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

COMMENT ON TABLE public.bronze_climate_raw IS
    'Camada Bronze com o payload bruto retornado pela Open-Meteo para cada cidade.';

COMMENT ON COLUMN public.bronze_climate_raw.raw_json IS
    'JSON bruto com todas as series solicitadas na API, incluindo os parametros necessarios para ET0.';

DO $$
BEGIN
    EXECUTE format('GRANT ALL PRIVILEGES ON TABLE public.bronze_climate_raw TO %I', current_user);
    EXECUTE format('ALTER TABLE public.bronze_climate_raw OWNER TO %I', current_user);
END $$;
