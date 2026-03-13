-- Tabela de ingestao bruta (Bronze Layer) para multiplas cidades
CREATE TABLE IF NOT EXISTS public.bronze_climate_raw (
    id SERIAL PRIMARY KEY,
    city TEXT NOT NULL,
    raw_json JSONB NOT NULL,
    ingestion_time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE public.bronze_climate_raw IS
    'Camada Bronze com o payload bruto retornado pela Open-Meteo para cada cidade.';

COMMENT ON COLUMN public.bronze_climate_raw.raw_json IS
    'JSON bruto com todas as series solicitadas na API, incluindo os parametros necessarios para ET0.';

COMMENT ON COLUMN public.bronze_climate_raw.ingestion_time IS
    'Timestamp real da ingestao com timezone preservado.';

COMMENT ON COLUMN public.bronze_climate_raw.created_at IS
    'Timestamp de criacao do registro com timezone preservado.';

DO $$
BEGIN
    EXECUTE format('GRANT ALL PRIVILEGES ON TABLE public.bronze_climate_raw TO %I', current_user);
    EXECUTE format('ALTER TABLE public.bronze_climate_raw OWNER TO %I', current_user);
END $$;
