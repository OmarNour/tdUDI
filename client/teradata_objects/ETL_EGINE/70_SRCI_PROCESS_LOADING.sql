REPLACE  PROCEDURE /*VER.04*/ GDEV1_ETL.SRCI_PROCESS_LOADING
    (
    IN 		i_TABLE_NAME  			VARCHAR(1000),
    IN 		I_RUN_ID 				BIGINT,
    IN		I_LOAD_ID				VARCHAR(500),
    OUT  	O_ROWS_INSERTED_COUNT	FLOAT,
    OUT  	O_ROWS_UPDATED_COUNT	FLOAT,
    OUT  	O_ROWS_DELETED_COUNT	FLOAT,
    OUT  	O_RETURN_CODE  			INTEGER,
    OUT  	O_RETURN_MSG 			VARCHAR(5000),
    OUT 	o_run_id 				BIGINT
    )
    BEGIN
	    DECLARE 	v_START_TIMESTAMP 			TIMESTAMP(6) default current_timestamp;
	    	    
		DECLARE		V_LAYER_NAME				VARCHAR(10) DEFAULT 'SRCI';
		DECLARE		V_PROCESS_NAME				VARCHAR(200) DEFAULT '';
		
        DECLARE 	V_RETURN_CODE  				INTEGER DEFAULT 0;
        DECLARE 	V_RETURN_MSG 				VARCHAR(5000) DEFAULT 'Process Completed Successfully';
        
        DECLARE 	V_INSERTED_ROWS_COUNT,V_UPDATED_ROWS_COUNT,V_DELETED_ROWS_COUNT,
        			V_ROWS_IN_1batch, V_ROWS_IN_DELTA, V_ROWS_COUNT, V_DBC_ROWS_COUNT				FLOAT DEFAULT 0;

        DECLARE		v_run_id, v_dbc_run_id		BIGINT;
        DECLARE 	V_SQL_SCRIPT_ID				INTEGER DEFAULT 0;
        DECLARE 	V_DBC_RETURN_CODE 			INTEGER;
		DECLARE 	V_DBC_RETURN_MSG  			VARCHAR(1000);
  
    				
        DECLARE    	V_SRC_DB					VARCHAR(500);
        DECLARE    	V_SRC_TABLE					VARCHAR(500);
        DECLARE    	V_TGT_DB					VARCHAR(500);
        DECLARE    	V_TGT_TABLE					VARCHAR(500);
        
     	
        DECLARE 	V_WITH_DATA_PI				VARCHAR(100) DEFAULT 'WITH DATA AND STATS PRIMARY INDEX';
		DECLARE 	V_WITH_DATA_UNIQUE_PI		VARCHAR(100) DEFAULT 'WITH DATA AND STATS UNIQUE PRIMARY INDEX';
	
		DECLARE		V_SOURCE_NAME			VARCHAR(500);
				    
		declare		V_KEY_COLs, V_PI_COLs
					, v_create_new_table
					, v_rename_current_table
					, v_rename_new_table
					, v_drop_old_table
					, v_drop_new_table 		varchar(2000) default '';
		
        DECLARE EXIT HANDLER FOR SQLEXCEPTION
        BEGIN 	
	        SET V_RETURN_CODE = SQLCODE;
			GET DIAGNOSTICS EXCEPTION 1 V_RETURN_MSG = MESSAGE_TEXT; 			
		
			SET o_RETURN_CODE  = V_RETURN_CODE;
			SET o_RETURN_MSG = V_RETURN_MSG;
			
			SET O_ROWS_INSERTED_COUNT 	= V_INSERTED_ROWS_COUNT;
			SET O_ROWS_UPDATED_COUNT 	= V_UPDATED_ROWS_COUNT;
			SET O_ROWS_DELETED_COUNT 	= V_DELETED_ROWS_COUNT;
			SET o_run_id 				= coalesce(v_run_id, i_run_id);
			
            INSERT INTO GDEV1_ETL.EXEC_PROCESS_LOGS  
            (RUN_ID, LOAD_ID, SOURCE_NAME, PROCESS_NAME, START_TIMESTAMP, END_TIMESTAMP, ROWS_INSERTED_COUNT, ROWS_UPDATED_COUNT, ROWS_DELETED_COUNT, ERROR_CODE, ERROR_MSG)  
            VALUES
            (coalesce(v_run_id,	i_run_id), i_LOAD_ID, V_SOURCE_NAME, V_LAYER_NAME||'_'||i_TABLE_NAME, v_START_TIMESTAMP, current_timestamp, V_INSERTED_ROWS_COUNT, V_UPDATED_ROWS_COUNT, V_DELETED_ROWS_COUNT, V_RETURN_CODE, V_RETURN_MSG);
        END;
        
        IF i_run_id IS NULL THEN 
			select GDEV1_ETL.generate_run_id()
			into v_run_id;
		ELSE 	
			SET v_run_id=	i_run_id;
		END IF;
		
        MAINBLOCK:
        BEGIN
	        SET V_PROCESS_NAME = V_LAYER_NAME||'_'||i_TABLE_NAME;
			
            SELECT SRC.SOURCE_NAME
            		,TBL.srci_src_db V_SRC_DB
            		,TBL.SRCI_DB  V_TGT_DB
		    FROM GDEV1_ETL.V_SOURCE_SYSTEM_TABLES TBL
		    	JOIN GDEV1_ETL.SOURCE_SYSTEMS SRC
		    	ON TBL.SOURCE_NAME=SRC.SOURCE_NAME
		    WHERE SRC.ACTIVE = 1
		    AND  SRC.STG_ACTIVE = 1
		    AND  SRC.ACTIVE = 1
		    AND  TBL.ACTIVE = 1
		    AND  TBL.TABLE_NAME = i_TABLE_NAME
		    
		    INTO V_SOURCE_NAME, V_SRC_DB, V_TGT_DB;
            
		    IF V_SOURCE_NAME IS NULL
            THEN
            	set V_RETURN_CODE = -1;
            	SET V_RETURN_MSG = 'Invalid Proccess!';
            	leave MAINBLOCK;
            end if;
            
            if V_SRC_DB is null or V_TGT_DB is null
	        then
	        	set V_RETURN_CODE = -1;
            	SET V_RETURN_MSG = 'INVALID source or target DB!';
            	leave MAINBLOCK;
	        end if;
            
			SELECT 
			 TRIM( TRAILING  ',' FROM (XMLAGG(trim(PI_Column)|| ',' ORDER BY PI_Column) (VARCHAR(10000)))) PI_Column
			from GDEV1_ETL.PICOLs
			WHERE LAYER_NAME = V_TGT_DB
			AND TABLE_NAME  = i_TABLE_NAME
			INTO V_PI_COLs;
			
            SELECT 
			 TRIM( TRAILING  ',' FROM (XMLAGG(trim(Key_Column)|| ',' ORDER BY Key_Column) (VARCHAR(10000)))) Key_Column
			from GDEV1_ETL.V_TRANSFORM_KEYCOL
			WHERE lyr_db = V_TGT_DB
			AND TABLE_NAME  = i_TABLE_NAME
			INTO V_KEY_COLs;
			
			if coalesce(V_KEY_COLs,'') = ''
			then
				set V_RETURN_CODE = -1;
            	SET V_RETURN_MSG = 'No keys defined for table '||i_TABLE_NAME||'!';
            	leave MAINBLOCK;
			end if;
			
			if coalesce(V_PI_COLs,'') = ''
			then
				set V_PI_COLs = V_KEY_COLs;
			end if;
			
			set v_drop_new_table = 'drop table '||V_TGT_DB||'.'||i_TABLE_NAME||'_new;';
			
			set v_create_new_table = 'create table '||V_TGT_DB||'.'||i_TABLE_NAME||'_new as
									( select * from '||V_SRC_DB||'.'||i_TABLE_NAME||') 
									'||V_WITH_DATA_PI||' ('||V_PI_COLs||');';
			
			set v_rename_current_table = 'rename table '||V_TGT_DB||'.'||i_TABLE_NAME||' as '||V_TGT_DB||'.'||i_TABLE_NAME||'_old;';
			set v_rename_new_table = 'rename table '||V_TGT_DB||'.'||i_TABLE_NAME||'_new as '||V_TGT_DB||'.'||i_TABLE_NAME||';';
			set v_drop_old_table = 'drop table '||V_TGT_DB||'.'||i_TABLE_NAME||'_old;';
			
			CALL GDEV1_ETL.DBC_SYSEXECSQL_WITH_LOG(v_drop_new_table,V_SQL_SCRIPT_ID,v_run_id,V_SOURCE_NAME,V_PROCESS_NAME,I_LOAD_ID,0/*1 log or 0 don't*/,V_DBC_RETURN_CODE,V_DBC_RETURN_MSG,V_DBC_ROWS_COUNT, v_dbc_run_id);
			CALL GDEV1_ETL.DBC_SYSEXECSQL_WITH_LOG(v_drop_old_table,V_SQL_SCRIPT_ID,v_run_id,V_SOURCE_NAME,V_PROCESS_NAME,I_LOAD_ID,0/*1 log or 0 don't*/,V_DBC_RETURN_CODE,V_DBC_RETURN_MSG,V_DBC_ROWS_COUNT, v_dbc_run_id);
			
			SET V_SQL_SCRIPT_ID = V_SQL_SCRIPT_ID + 1;
			CALL GDEV1_ETL.DBC_SYSEXECSQL_WITH_LOG(v_create_new_table,V_SQL_SCRIPT_ID,v_run_id,V_SOURCE_NAME,V_PROCESS_NAME,I_LOAD_ID,1/*1 log or 0 don't*/,V_DBC_RETURN_CODE,V_DBC_RETURN_MSG,V_DBC_ROWS_COUNT, v_dbc_run_id);
			IF V_DBC_RETURN_CODE <> 0 THEN
				SET V_RETURN_CODE = V_DBC_RETURN_CODE;
				SET V_RETURN_MSG = V_DBC_RETURN_MSG;
				LEAVE MainBlock;
			END IF;
			
			SET V_SQL_SCRIPT_ID = V_SQL_SCRIPT_ID + 1;
			CALL GDEV1_ETL.DBC_SYSEXECSQL_WITH_LOG(v_rename_current_table,V_SQL_SCRIPT_ID,v_run_id,V_SOURCE_NAME,V_PROCESS_NAME,I_LOAD_ID,1/*1 log or 0 don't*/,V_DBC_RETURN_CODE,V_DBC_RETURN_MSG,V_DBC_ROWS_COUNT, v_dbc_run_id);
			IF V_DBC_RETURN_CODE <> 0 THEN
				SET V_RETURN_CODE = V_DBC_RETURN_CODE;
				SET V_RETURN_MSG = V_DBC_RETURN_MSG;
				LEAVE MainBlock;
			END IF;
			
			SET V_SQL_SCRIPT_ID = V_SQL_SCRIPT_ID + 1;
			CALL GDEV1_ETL.DBC_SYSEXECSQL_WITH_LOG(v_rename_new_table,V_SQL_SCRIPT_ID,v_run_id,V_SOURCE_NAME,V_PROCESS_NAME,I_LOAD_ID,1/*1 log or 0 don't*/,V_DBC_RETURN_CODE,V_DBC_RETURN_MSG,V_DBC_ROWS_COUNT, v_dbc_run_id);
			IF V_DBC_RETURN_CODE <> 0 THEN
				SET V_RETURN_CODE = V_DBC_RETURN_CODE;
				SET V_RETURN_MSG = V_DBC_RETURN_MSG;
				LEAVE MainBlock;
			END IF;
			
			SET V_SQL_SCRIPT_ID = V_SQL_SCRIPT_ID + 1;
			CALL GDEV1_ETL.DBC_SYSEXECSQL_WITH_LOG(v_drop_old_table,V_SQL_SCRIPT_ID,v_run_id,V_SOURCE_NAME,V_PROCESS_NAME,I_LOAD_ID,1/*1 log or 0 don't*/,V_DBC_RETURN_CODE,V_DBC_RETURN_MSG,V_DBC_ROWS_COUNT, v_dbc_run_id);
			IF V_DBC_RETURN_CODE <> 0 THEN
				SET V_RETURN_CODE = V_DBC_RETURN_CODE;
				SET V_RETURN_MSG = V_DBC_RETURN_MSG;
				LEAVE MainBlock;
			END IF;
			
			CALL GDEV1_ETL.COUNT_ROWS(V_TGT_DB, i_TABLE_NAME, '',V_INSERTED_ROWS_COUNT, V_DBC_RETURN_CODE, V_DBC_RETURN_MSG);
			IF V_DBC_RETURN_CODE <> 0 THEN
				SET V_RETURN_CODE = V_DBC_RETURN_CODE;
				SET V_RETURN_MSG = V_DBC_RETURN_MSG;
				LEAVE MainBlock;
			END IF;
			
        END;
                                    
        INSERT INTO GDEV1_ETL.EXEC_PROCESS_LOGS  
        (RUN_ID, LOAD_ID, SOURCE_NAME, PROCESS_NAME, START_TIMESTAMP, END_TIMESTAMP, ROWS_INSERTED_COUNT, ROWS_UPDATED_COUNT, ROWS_DELETED_COUNT, ERROR_CODE, ERROR_MSG)  
        VALUES
        (v_run_id, i_LOAD_ID, V_SOURCE_NAME, V_PROCESS_NAME, v_START_TIMESTAMP, current_timestamp, V_INSERTED_ROWS_COUNT, V_UPDATED_ROWS_COUNT, V_DELETED_ROWS_COUNT, V_RETURN_CODE, V_RETURN_MSG);
        
                                                                                         
        SET O_RETURN_CODE = V_RETURN_CODE;
        SET O_RETURN_MSG = V_RETURN_MSG;
        SET O_ROWS_INSERTED_COUNT	= V_INSERTED_ROWS_COUNT;
    	SET	O_ROWS_UPDATED_COUNT	= V_UPDATED_ROWS_COUNT;
    	SET	O_ROWS_DELETED_COUNT	= V_DELETED_ROWS_COUNT;
    	set o_run_id				= v_run_id;
    END;