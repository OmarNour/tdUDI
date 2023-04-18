REPLACE  PROCEDURE /*VER.01*/ GDEV1P_PP.SRCI_PROCESS_LOADING
    (
    IN 		i_TABLE_NAME  			VARCHAR(1000),
    IN 		I_RUN_ID 				BIGINT,
    IN		I_LOAD_ID				VARCHAR(500),
    OUT  	O_ROWS_INSERTED_COUNT	FLOAT,
    OUT  	O_ROWS_UPDATED_COUNT	FLOAT,
    OUT  	O_ROWS_DELETED_COUNT	FLOAT,
    OUT  	O_RETURN_CODE  			INTEGER,
    OUT  	O_RETURN_MSG 			VARCHAR(5000)
    )
    BEGIN
        DECLARE 	V_RETURN_CODE  				INTEGER DEFAULT 0;
        DECLARE 	V_RETURN_MSG 				VARCHAR(5000) DEFAULT 'Process Completed Successfully';
        
        DECLARE 	V_INSERTED_ROWS_COUNT,V_UPDATED_ROWS_COUNT,V_DELETED_ROWS_COUNT,
        			V_ROWS_IN_1batch, V_ROWS_IN_DELTA, V_ROWS_COUNT, V_DBC_ROWS_COUNT				FLOAT DEFAULT 0;

        DECLARE		v_run_id					BIGINT;
        DECLARE 	V_SQL_SCRIPT_ID				INTEGER DEFAULT 0;
        DECLARE 	V_DBC_RETURN_CODE 			INTEGER;
		DECLARE 	V_DBC_RETURN_MSG  			VARCHAR(1000);
  
    				
        DECLARE    	V_SRC_DB					VARCHAR(500) DEFAULT 'GDEV1V_SRCI';
        DECLARE    	V_SRC_TABLE					VARCHAR(500);
        DECLARE    	V_TGT_DB					VARCHAR(500) DEFAULT 'GDEV1T_SRCI';
        DECLARE    	V_TGT_TABLE					VARCHAR(500);
        
     	
        DECLARE 	V_WITH_DATA_PI				VARCHAR(100) DEFAULT 'WITH DATA AND STATS PRIMARY INDEX';
		DECLARE 	V_WITH_DATA_UNIQUE_PI		VARCHAR(100) DEFAULT 'WITH DATA AND STATS UNIQUE PRIMARY INDEX';
	
		DECLARE		V_SOURCE_NAME			VARCHAR(500);
				    
		declare		V_KEY_COLs
					, v_create_new_table
					, v_rename_current_table
					, v_rename_new_table
					, v_drop_old_table
					, v_drop_new_table 		varchar(2000) default '';
		
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
            SELECT SRC.SOURCE_NAME
		    FROM GDEV1T_GCFR.STG_TABLES TBL
		    	JOIN GDEV1T_GCFR.SOURCE_SYSTEMS SRC
		    	ON TBL.SOURCE_NAME=SRC.SOURCE_NAME
		    WHERE SRC.ACTIVE = 1
		    AND  SRC.STG_ACTIVE = 1
		    AND  SRC.ACTIVE = 1
		    AND  TBL.ACTIVE = 1
		    AND  TBL.TABLE_NAME = i_TABLE_NAME
		    
		    INTO V_SOURCE_NAME;
            
		    IF V_SOURCE_NAME IS NULL
            THEN
            	set V_RETURN_CODE = -1;
            	SET V_RETURN_MSG = 'Invalid Proccess!';
            	leave MAINBLOCK;
            end if;
            
            SELECT 
			 TRIM( TRAILING  ',' FROM (XMLAGG(trim(Key_Column)|| ',' ORDER BY Key_Column) (VARCHAR(10000)))) Key_Column
			from GDEV1t_GCFR.TRANSFORM_KEYCOL
			WHERE DB_NAME = V_TGT_DB
			AND TABLE_NAME  = i_TABLE_NAME
			INTO V_KEY_COLs;
			
			if coalesce(V_KEY_COLs,'') = ''
			then
				set V_RETURN_CODE = -1;
            	SET V_RETURN_MSG = 'No keys defined for table '||i_TABLE_NAME||'!';
            	leave MAINBLOCK;
			end if;
			
			set v_drop_new_table = 'drop table '||V_TGT_DB||'.'||i_TABLE_NAME||'_new;';
			
			set v_create_new_table = 'create table '||V_TGT_DB||'.'||i_TABLE_NAME||'_new as
									( select * from '||V_SRC_DB||'.'||i_TABLE_NAME||') 
									'||V_WITH_DATA_UNIQUE_PI||' ('||V_KEY_COLs||');';
			
			set v_rename_current_table = 'rename table '||V_TGT_DB||'.'||i_TABLE_NAME||' as '||V_TGT_DB||'.'||i_TABLE_NAME||'_old;';
			set v_rename_new_table = 'rename table '||V_TGT_DB||'.'||i_TABLE_NAME||'_new as '||V_TGT_DB||'.'||i_TABLE_NAME||';';
			set v_drop_old_table = 'drop table '||V_TGT_DB||'.'||i_TABLE_NAME||'_old;';
			
			CALL GDEV1P_PP.DBC_SYSEXECSQL_WITH_LOG(v_drop_new_table,V_SQL_SCRIPT_ID,v_run_id,V_SOURCE_NAME,i_TABLE_NAME,I_LOAD_ID,0/*1 log or 0 don't*/,V_DBC_RETURN_CODE,V_DBC_RETURN_MSG,V_DBC_ROWS_COUNT);
			CALL GDEV1P_PP.DBC_SYSEXECSQL_WITH_LOG(v_drop_old_table,V_SQL_SCRIPT_ID,v_run_id,V_SOURCE_NAME,i_TABLE_NAME,I_LOAD_ID,0/*1 log or 0 don't*/,V_DBC_RETURN_CODE,V_DBC_RETURN_MSG,V_DBC_ROWS_COUNT);
			
			SET V_SQL_SCRIPT_ID = V_SQL_SCRIPT_ID + 1;
			CALL GDEV1P_PP.DBC_SYSEXECSQL_WITH_LOG(v_create_new_table,V_SQL_SCRIPT_ID,v_run_id,V_SOURCE_NAME,i_TABLE_NAME,I_LOAD_ID,1/*1 log or 0 don't*/,V_DBC_RETURN_CODE,V_DBC_RETURN_MSG,V_DBC_ROWS_COUNT);
			IF V_DBC_RETURN_CODE <> 0 THEN
				SET V_RETURN_CODE = V_DBC_RETURN_CODE;
				SET V_RETURN_MSG = V_DBC_RETURN_MSG;
				LEAVE MainBlock;
			END IF;
			
			SET V_SQL_SCRIPT_ID = V_SQL_SCRIPT_ID + 1;
			CALL GDEV1P_PP.DBC_SYSEXECSQL_WITH_LOG(v_rename_current_table,V_SQL_SCRIPT_ID,v_run_id,V_SOURCE_NAME,i_TABLE_NAME,I_LOAD_ID,1/*1 log or 0 don't*/,V_DBC_RETURN_CODE,V_DBC_RETURN_MSG,V_DBC_ROWS_COUNT);
			IF V_DBC_RETURN_CODE <> 0 THEN
				SET V_RETURN_CODE = V_DBC_RETURN_CODE;
				SET V_RETURN_MSG = V_DBC_RETURN_MSG;
				LEAVE MainBlock;
			END IF;
			
			SET V_SQL_SCRIPT_ID = V_SQL_SCRIPT_ID + 1;
			CALL GDEV1P_PP.DBC_SYSEXECSQL_WITH_LOG(v_rename_new_table,V_SQL_SCRIPT_ID,v_run_id,V_SOURCE_NAME,i_TABLE_NAME,I_LOAD_ID,1/*1 log or 0 don't*/,V_DBC_RETURN_CODE,V_DBC_RETURN_MSG,V_DBC_ROWS_COUNT);
			IF V_DBC_RETURN_CODE <> 0 THEN
				SET V_RETURN_CODE = V_DBC_RETURN_CODE;
				SET V_RETURN_MSG = V_DBC_RETURN_MSG;
				LEAVE MainBlock;
			END IF;
			
			SET V_SQL_SCRIPT_ID = V_SQL_SCRIPT_ID + 1;
			CALL GDEV1P_PP.DBC_SYSEXECSQL_WITH_LOG(v_drop_old_table,V_SQL_SCRIPT_ID,v_run_id,V_SOURCE_NAME,i_TABLE_NAME,I_LOAD_ID,1/*1 log or 0 don't*/,V_DBC_RETURN_CODE,V_DBC_RETURN_MSG,V_DBC_ROWS_COUNT);
			IF V_DBC_RETURN_CODE <> 0 THEN
				SET V_RETURN_CODE = V_DBC_RETURN_CODE;
				SET V_RETURN_MSG = V_DBC_RETURN_MSG;
				LEAVE MainBlock;
			END IF;
			
			CALL GDEV1P_PP.COUNT_ROWS(V_TGT_DB, i_TABLE_NAME, '',V_INSERTED_ROWS_COUNT, V_DBC_RETURN_CODE, V_DBC_RETURN_MSG);
			IF V_DBC_RETURN_CODE <> 0 THEN
				SET V_RETURN_CODE = V_DBC_RETURN_CODE;
				SET V_RETURN_MSG = V_DBC_RETURN_MSG;
				LEAVE MainBlock;
			END IF;
			
        END;
                                                                                        
                                                                                         
        SET O_RETURN_CODE = V_RETURN_CODE;
        SET O_RETURN_MSG = V_RETURN_MSG;
        SET O_ROWS_INSERTED_COUNT	= V_INSERTED_ROWS_COUNT;
    	SET	O_ROWS_UPDATED_COUNT	= V_UPDATED_ROWS_COUNT;
    	SET	O_ROWS_DELETED_COUNT	= V_DELETED_ROWS_COUNT;
    END;