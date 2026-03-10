DO $$
BEGIN
    EXECUTE format('ALTER DATABASE %I OWNER TO %I', current_database(), current_user);
    EXECUTE format('GRANT ALL PRIVILEGES ON DATABASE %I TO %I', current_database(), current_user);
    EXECUTE format('GRANT CONNECT ON DATABASE %I TO %I', current_database(), current_user);
END $$;