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
    
    RETURN CAST(oreplace(oreplace(oreplace(oreplace(oreplace( CAST(CURRENT_TIMESTAMP(6) AS VARCHAR(24)),'-',''),' ',''),':',''),'.',''),'+','')AS BIGINT);