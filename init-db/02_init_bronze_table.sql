-- Tabela de ingestão bruta (Bronze Layer) para múltiplas cidades
CREATE TABLE IF NOT EXISTS public.bronze_climate_raw (
    id SERIAL PRIMARY KEY,
    city TEXT NOT NULL,
    raw_json JSONB NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

DO $$
BEGIN
    EXECUTE format('GRANT ALL PRIVILEGES ON TABLE public.bronze_climate_raw TO %I', current_user);
    EXECUTE format('ALTER TABLE public.bronze_climate_raw OWNER TO %I', current_user);
END $$;