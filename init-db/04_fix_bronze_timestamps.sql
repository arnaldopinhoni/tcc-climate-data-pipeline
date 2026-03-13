-- Execute manualmente em uma base ja existente.
-- Este script assume que os valores atuais de ingestion_time/created_at
-- foram gravados como horario UTC em colunas sem timezone.
-- Exemplo: 2026-03-13 15:49 precisa virar 2026-03-13 12:49-03.

BEGIN;

ALTER TABLE public.bronze_climate_raw
    ALTER COLUMN ingestion_time TYPE TIMESTAMPTZ
    USING ingestion_time AT TIME ZONE 'UTC';

ALTER TABLE public.bronze_climate_raw
    ALTER COLUMN created_at TYPE TIMESTAMPTZ
    USING created_at AT TIME ZONE 'UTC';

ALTER TABLE public.bronze_climate_raw
    ALTER COLUMN ingestion_time SET DEFAULT NOW(),
    ALTER COLUMN ingestion_time SET NOT NULL,
    ALTER COLUMN created_at SET DEFAULT NOW(),
    ALTER COLUMN created_at SET NOT NULL;

COMMIT;
