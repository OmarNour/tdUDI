REPLACE  PROCEDURE /*VER.01*/ GDEV1P_PP.STG_PROCESS_LOADING
    (
    IN 		i_TABLE_NAME  			VARCHAR(1000),
    IN 		I_RUN_ID 				BIGINT,
    IN		I_LOAD_ID				VARCHAR(500),
    OUT  	O_ROWS_COUNT  			BIGINT,
    OUT  	O_RETURN_CODE  			INTEGER,
    OUT  	O_RETURN_MSG 			VARCHAR(5000)
    )
    BEGIN
        DECLARE 	V_RETURN_CODE  				INTEGER DEFAULT 0;
        DECLARE 	V_RETURN_MSG 				VARCHAR(5000) DEFAULT 'Process Completed Successfully';
        DECLARE 	V_ROWS_IN_TMP, V_ROWS_IN_BASE, V_ROWS_IN_DELTA, V_ROWS_COUNT				FLOAT DEFAULT 0;
        DECLARE		v_run_id					BIGINT;
        DECLARE 	V_SQL_SCRIPT_ID				INTEGER DEFAULT 0;
        DECLARE 	V_DBC_RETURN_CODE 			INTEGER;
		DECLARE 	V_DBC_RETURN_MSG  			VARCHAR(1000);
		DECLARE 	V_DBC_ROWS_COUNT			FLOAT;
    	DECLARE 	v_statment					CLOB;                                
        DECLARE    	V_SRC_DB					VARCHAR(500);
        DECLARE    	V_SRC_TABLE					VARCHAR(500);
        DECLARE    	V_TGT_DB					VARCHAR(500);
        DECLARE    	V_TGT_TABLE					VARCHAR(500);
        DECLARE    	V_APPLY_TYPE				VARCHAR(500);
        DECLARE 	V_NULLABLE_COLUMN			VARCHAR(200);
        
        DECLARE		V_BATCH_ID					VARCHAR(150)	DEFAULT 'BATCH_ID';
        DECLARE	   	V_REF_KEY   				VARCHAR(150)	DEFAULT 'REF_KEY';
        DECLARE	   	V_MODIFICATION_TYPE 		VARCHAR(150)	DEFAULT 'MODIFICATION_TYPE';
        
     	
        DECLARE 	V_WITH_DATA_PI				VARCHAR(100) DEFAULT 'WITH DATA AND STATS PRIMARY INDEX';
		DECLARE 	V_WITH_DATA_UNIQUE_PI		VARCHAR(100) DEFAULT 'WITH DATA AND STATS UNIQUE PRIMARY INDEX';
	
		DECLARE		v_table_name_1batch,
					V_SOURCE_NAME,
				    V_SOURCE_MODE,
				    V_REJECTION_TABLE_NAME,
				    V_BUSINESS_RULES_TABLE_NAME,
				    V_SOURCE_DB					VARCHAR(500);
				    
		DECLARE		V_IS_TARANSACTIOANL			INTEGER;
		
        DECLARE EXIT HANDLER FOR SQLEXCEPTION
        BEGIN 	
            SET O_RETURN_CODE = SQLCODE;
            GET DIAGNOSTICS EXCEPTION 1 O_RETURN_MSG = MESSAGE_TEXT;
        END;
        
        IF i_run_id IS NULL THEN 
			select GDEV1P_FF.generate_run_id()
			into v_run_id;
		ELSE 	
			SET v_run_id=	i_run_id;
		END IF;
		
        MAINBLOCK:
        BEGIN
            SELECT 
            SRC.SOURCE_NAME,
		    SRC.SOURCE_MODE,
		    SRC.REJECTION_TABLE_NAME,
		    SRC.BUSINESS_RULES_TABLE_NAME,
		    SRC.SOURCE_DB,
		    TBL.IS_TARANSACTIOANL
		    FROM GDEV1T_GCFR.STG_TABLES TBL
		    	JOIN GDEV1T_GCFR.SOURCE_SYSTEMS SRC
		    	ON TBL.SOURCE_NAME=SRC.SOURCE_NAME
		    WHERE SRC.ACTIVE = 1
		    AND  SRC.STG_ACTIVE = 1
		    AND  SRC.ACTIVE = 1
		    AND  TBL.ACTIVE = 1
		    AND  TBL.TABLE_NAME = i_TABLE_NAME
		    
		    INTO V_SOURCE_NAME,
		    V_SOURCE_MODE,
		    V_REJECTION_TABLE_NAME,
		    V_BUSINESS_RULES_TABLE_NAME,
		    V_SOURCE_DB,
		    V_IS_TARANSACTIOANL;
            
		    set v_table_name_1batch = i_TABLE_NAME||'_1batch';
		    
            IF V_SOURCE_NAME IS NULL
            THEN
            	set V_RETURN_CODE = -1;
            	SET V_RETURN_MSG = 'Invalid Proccess!';
            	leave MAINBLOCK;
            end if;
            
            
			
			SET V_SQL_SCRIPT_ID = V_SQL_SCRIPT_ID + 1;
			CALL GDEV1P_PP.DBC_SYSEXECSQL_WITH_LOG('BT',V_SQL_SCRIPT_ID,v_run_id,V_SOURCE_NAME,i_TABLE_NAME,I_LOAD_ID,1/*1 log or 0 don't*/,V_DBC_RETURN_CODE,V_DBC_RETURN_MSG,V_DBC_ROWS_COUNT);
			IF V_DBC_RETURN_CODE <> 0 THEN
				SET V_RETURN_CODE = V_DBC_RETURN_CODE;
				SET V_RETURN_MSG = V_DBC_RETURN_MSG;
				LEAVE MainBlock;
			END IF;	
			
			
			
			SET V_SQL_SCRIPT_ID = V_SQL_SCRIPT_ID + 1;
			CALL GDEV1P_PP.DBC_SYSEXECSQL_WITH_LOG('ET',V_SQL_SCRIPT_ID,v_run_id,V_SOURCE_NAME,i_TABLE_NAME,I_LOAD_ID,1/*1 log or 0 don't*/,V_DBC_RETURN_CODE,V_DBC_RETURN_MSG,V_DBC_ROWS_COUNT);
			IF V_DBC_RETURN_CODE <> 0 THEN
				SET V_RETURN_CODE = V_DBC_RETURN_CODE;
				SET V_RETURN_MSG = V_DBC_RETURN_MSG;
				LEAVE MainBlock;
			END IF;
        END;
                                                                                        
                                                                                         
        SET O_RETURN_CODE = V_RETURN_CODE;
        SET O_RETURN_MSG = V_RETURN_MSG;
        SET O_ROWS_COUNT = V_ROWS_COUNT;
    END;