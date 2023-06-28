REPLACE FUNCTION GDEV1_ETL.generate_run_id ()
    RETURNS BIGINT
    LANGUAGE SQL
    DETERMINISTIC
    CONTAINS SQL
    SPECIFIC GDEV1_ETL.generate_run_id 
    CALLED ON NULL INPUT
    SQL SECURITY DEFINER
    COLLATION INVOKER
    INLINE TYPE 1
    
    -- Function to generate a unique run ID based on the current timestamp
    -- and removing any non-numeric characters
    
    RETURN CAST(oreplace(oreplace(oreplace(oreplace(oreplace( CAST(CURRENT_TIMESTAMP(6) AS VARCHAR(24)),'-',''),' ',''),':',''),'.',''),'+','')AS BIGINT);