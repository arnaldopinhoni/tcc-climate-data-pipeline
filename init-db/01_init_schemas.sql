-- Criação dos schemas do projeto
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

DO $$
BEGIN
    EXECUTE format('GRANT ALL PRIVILEGES ON SCHEMA bronze TO %I', current_user);
    EXECUTE format('GRANT ALL PRIVILEGES ON SCHEMA silver TO %I', current_user);
    EXECUTE format('GRANT ALL PRIVILEGES ON SCHEMA gold TO %I', current_user);

    EXECUTE format('ALTER SCHEMA bronze OWNER TO %I', current_user);
    EXECUTE format('ALTER SCHEMA silver OWNER TO %I', current_user);
    EXECUTE format('ALTER SCHEMA gold OWNER TO %I', current_user);
END $$;