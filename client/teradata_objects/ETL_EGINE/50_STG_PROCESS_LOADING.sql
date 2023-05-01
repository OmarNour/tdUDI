REPLACE  PROCEDURE /*VER.01*/ GDEV1_ETL.STG_PROCESS_LOADING
    (
    IN 		i_TABLE_NAME  			VARCHAR(1000),
    IN 		I_RUN_ID 				BIGINT,
    IN		I_LOAD_ID				VARCHAR(500),
    IN		I_RELOAD				INTEGER, /* 1 RELOAD ALL LOADS WHICH LOADED SUCCESSFULLY BEFORE, 0 IGNORE*/
    IN		I_ALL_BATCHES			INTEGER, /* 1 INCLUDES BATCH_ID COLUMN AS A KEY, SO IT GRASPS ALL DATA FROM ALL BATCHES, 0 IGNORE*/
    OUT  	O_ROWS_INSERTED_COUNT	FLOAT,
    OUT  	O_ROWS_UPDATED_COUNT	FLOAT,
    OUT  	O_ROWS_DELETED_COUNT	FLOAT,
    OUT  	O_RETURN_CODE  			INTEGER,
    OUT  	O_RETURN_MSG 			VARCHAR(5000),
    OUT 	o_run_id 				BIGINT
    )
    BEGIN
	    DECLARE 	v_START_TIMESTAMP 			TIMESTAMP(6) default current_timestamp;
	    
        DECLARE 	V_RETURN_CODE  				INTEGER DEFAULT 0;
        DECLARE 	V_RETURN_MSG 				VARCHAR(5000) DEFAULT 'Process Completed Successfully';
        DECLARE 	V_INSERTED_ROWS_COUNT,V_UPDATED_ROWS_COUNT,V_DELETED_ROWS_COUNT,
        			V_ROWS_IN_1batch, V_ROWS_IN_DELTA, V_ROWS_COUNT				FLOAT DEFAULT 0;
        DECLARE		v_run_id, v_dbc_run_id		BIGINT;
        DECLARE 	V_SQL_SCRIPT_ID				INTEGER DEFAULT 0;
        DECLARE 	V_DBC_RETURN_CODE 			INTEGER;
		DECLARE 	V_DBC_RETURN_MSG  			VARCHAR(1000);
		DECLARE 	V_DBC_ROWS_COUNT			FLOAT;
    	DECLARE 	v_drop_1batch
    				, v_drop_delta
    				, v_update_tgt
    				, v_insert_into_tgt
    				, v_create_1batch
    				, v_create_delta	CLOB;    
    				
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
	
		DECLARE		v_delta_TBL_NAME,
					v_table_name_1batch,
					V_SOURCE_NAME,
				    V_LOADING_MODE,
				    V_REJECTION_TABLE_NAME,
				    V_BUSINESS_RULES_TABLE_NAME VARCHAR(500);
				    
		DECLARE		V_IS_TARANSACTIOANL, v_large_object_count			INTEGER default 0;
		declare		v_online_load_id_filter,
					v_valid_batches_filter,
					V_UPDATE_ALL_TO_D,V_ALL_COLS,
					V_KEY_COLs, V_QUALIFY, v_, v_keys_eql, v_delta_keys_eql,
					v_set_non_key_cols, v_valid_data_filter,
					v_delta_COLS varchar(2000) default '';
		
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
            (coalesce(v_run_id,	i_run_id), i_LOAD_ID, V_SOURCE_NAME, i_TABLE_NAME, v_START_TIMESTAMP, current_timestamp, V_INSERTED_ROWS_COUNT, V_UPDATED_ROWS_COUNT, V_DELETED_ROWS_COUNT, V_RETURN_CODE, V_RETURN_MSG);
        END;
        
        IF i_run_id IS NULL THEN 
			select GDEV1_ETL.generate_run_id()
			into v_run_id;
		ELSE 	
			SET v_run_id=	i_run_id;
		END IF;
		
        MAINBLOCK:
        BEGIN
	        
	        SELECT 
            SRC.SOURCE_NAME,
		    SRC.LOADING_MODE,
		    SRC.REJECTION_TABLE_NAME,
		    SRC.BUSINESS_RULES_TABLE_NAME,
		    SRC.lyr_db SOURCE_DB,
		    TBL.STG_t_db TGT_DB,
		    TBL.IS_TARANSACTIOANL
		    FROM GDEV1_ETL.V_SOURCE_SYSTEM_TABLES TBL
		    	JOIN GDEV1_ETL.V_SOURCE_SYSTEMS SRC
		    	ON TBL.SOURCE_NAME=SRC.SOURCE_NAME
		    	and SRC.lyr_db = TBL.stg_src_db
		    WHERE SRC.ACTIVE = 1
		    AND  SRC.STG_ACTIVE = 1
		    AND  SRC.ACTIVE = 1
		    AND  TBL.ACTIVE = 1
		    AND  TBL.TABLE_NAME = i_TABLE_NAME
		    
		    INTO V_SOURCE_NAME,
		    V_LOADING_MODE,
		    V_REJECTION_TABLE_NAME,
		    V_BUSINESS_RULES_TABLE_NAME,
		    V_SRC_DB,
		    V_TGT_DB,
		    V_IS_TARANSACTIOANL;
            
		    IF V_SOURCE_NAME IS NULL
            THEN
            	set V_RETURN_CODE = -1;
            	SET V_RETURN_MSG = 'Invalid Proccess!';
            	leave MAINBLOCK;
            end if;
            
            if V_TGT_DB is null
	        then
	        	set V_RETURN_CODE = -1;
            	SET V_RETURN_MSG = 'Parameter STG_T_DB is not defined in PARAMETERS table!';
            	leave MAINBLOCK;
	        end if;
	        
            SELECT 
			 TRIM( TRAILING  ',' FROM (XMLAGG(trim(Key_Column)|| ',' ORDER BY Key_Column) (VARCHAR(10000)))) Key_Column
			, trim(XMLAGG( 'TGT.' || trim(Key_Column)|| ' = SRC.' ||trim(Key_Column) || ' AND ' ORDER BY Key_Column) (VARCHAR(10000))) _keys_eql
			, left(_keys_eql,(length(_keys_eql)-3)(int)) Keys_eql
			from GDEV1_ETL.V_TRANSFORM_KEYCOL
			WHERE lyr_db = V_TGT_DB
			AND TABLE_NAME  = i_TABLE_NAME
			INTO V_KEY_COLs, v_, v_keys_eql;
			
			if coalesce(V_KEY_COLs,'') = ''
			then
				set V_RETURN_CODE = -1;
            	SET V_RETURN_MSG = 'No keys defined for table '||i_TABLE_NAME||'!';
            	leave MAINBLOCK;
			end if;
			
			set v_delta_keys_eql = v_keys_eql;
			
			IF I_ALL_BATCHES = 1
			THEN
				SET V_KEY_COLs = V_KEY_COLs || ', ' || V_BATCH_ID;
				SET v_keys_eql = v_keys_eql || ' AND TGT.' || V_BATCH_ID || ' = SRC.' || V_BATCH_ID;
				SET V_QUALIFY = 'QUALIFY ROW_NUMBER()  OVER(PARTITION BY '||V_KEY_COLs||' ORDER BY MODIFICATION_TYPE DESC,REF_KEY DESC)=1';
			ELSE
				SET V_QUALIFY = 'QUALIFY ROW_NUMBER()  OVER(PARTITION BY '||V_KEY_COLs||' ORDER BY BATCH_ID DESC,MODIFICATION_TYPE DESC,REF_KEY DESC)=1';
			END IF;
			
			SELECT 
			TRIM( TRAILING  ',' FROM (XMLAGG(trim(COLUMNNAME)|| ' = SRC.' ||trim(COLUMNNAME) || ',' ORDER BY COLUMNNAME) (VARCHAR(10000)))) SET_COLS
			FROM DBC.COLUMNSV v
			WHERE DATABASENAME = V_TGT_DB
			AND TABLENAME  = i_TABLE_NAME
			and COLUMNNAME not in ('INS_DTTM' ,'UPD_DTTM')
			and not exists (select 1 
							from GDEV1_ETL.TRANSFORM_KEYCOL k 
							where k.TABLE_NAME = v.DATABASENAME 
							and k.TABLE_NAME = v.TABLENAME
							and k.Key_Column = v.COLUMNNAME)
			AND (coalesce(I_ALL_BATCHES,0)=0 OR (I_ALL_BATCHES=1 AND COLUMNNAME<>V_BATCH_ID))
			GROUP BY DATABASENAME, TABLENAME
			INTO v_set_non_key_cols;
			
			SELECT 
			TRIM( TRAILING  ',' FROM (XMLAGG(trim(COLUMNNAME)||',' ORDER BY COLUMNNAME) (VARCHAR(10000)))) COLS
			FROM DBC.COLUMNSV v
			WHERE DATABASENAME = V_TGT_DB
			AND TABLENAME  = i_TABLE_NAME
			and COLUMNNAME not in ('INS_DTTM' ,'UPD_DTTM', 'MODIFICATION_TYPE', 'LOAD_ID', 'BATCH_ID', 'REF_KEY')
			GROUP BY DATABASENAME, TABLENAME
			INTO v_delta_COLS;
			
			SELECT 
			TRIM( TRAILING  ',' FROM (XMLAGG(trim(COLUMNNAME)|| ',' ORDER BY COLUMNNAME) (VARCHAR(10000)))) COLS
			FROM DBC.COLUMNSV v
			WHERE DATABASENAME = V_TGT_DB
			AND TABLENAME  = i_TABLE_NAME
			and COLUMNNAME not in ('INS_DTTM' ,'UPD_DTTM')
			GROUP BY DATABASENAME, TABLENAME
			INTO V_ALL_COLS;
			
			select 
			count(1) cnt
			FROM DBC.columnsV 
			where DATABASENAME = V_TGT_DB
			and tablename=i_TABLE_NAME
			and ColumnType = 'CO'
			into v_large_object_count;
			
		    set v_table_name_1batch = i_TABLE_NAME||'_1batch';
		    set v_delta_TBL_NAME = v_table_name_1batch||'_delta';
		    
		    if V_REJECTION_TABLE_NAME is not null and V_BUSINESS_RULES_TABLE_NAME is not null
		    then
			    set v_valid_data_filter = 
					'and not exists
						(
							sel 1 
							from '||V_SRC_DB||'.'||V_REJECTION_TABLE_NAME||' r  
							where r.REF_KEY  = x.REF_KEY
							and exists (
											select 1 
											from '||V_SRC_DB||'.'||V_BUSINESS_RULES_TABLE_NAME||' br 
											where r.RULE_ID = br.RULE_ID 
											and br.active = 1 /*hard rejection rules*/							
										)
						)';
			end if;
            
            if V_LOADING_MODE = 'ONLINE'
            then
            	if i_LOAD_ID is not null 
				then
					set v_online_load_id_filter = ' and x.LOAD_ID = '''||i_LOAD_ID||'''';		
				end if;
				
				if I_RELOAD = 1
				then
					set v_online_load_id_filter = v_online_load_id_filter ||
													' and exists (
																select 1 
																from GDEV1_ETL.EXEC_SOURCE_LOGS c 
																where c.LOAD_ID = x.LOAD_ID 
																and c.DELETED <> 0
																and c.STG_DONE = 1
																)';		
				end if;
				
				set v_valid_batches_filter = 
						' and exists 
						(
							SELECT 1
							FROM GDEV1_ETL.CDC_AUDIT adt
							WHERE adt.PROCESSED = 0
							AND adt.LOAD_ID = x.LOAD_ID
							AND adt.SOURCE_NAME = '''||V_SOURCE_NAME||'''
							AND adt.TABLE_NAME = '''||i_TABLE_NAME||'''
						)';	
					
            else -- 'OFFLINE'
            	SET V_UPDATE_ALL_TO_D = 'update '||V_TGT_DB||'.'||i_TABLE_NAME||' set modification_type = ''D''; ';
            end if;
            
			
            set v_drop_1batch = 'DROP TABLE '||v_table_name_1batch||';';
            set v_drop_delta = 'DROP TABLE '||v_delta_TBL_NAME||';';
            
            set v_create_1batch = 
			'create multiset volatile TABLE '||v_table_name_1batch||'  , no fallback as 
			(
				sel * from '||V_SRC_DB||'.'||i_TABLE_NAME||' x
				where 1=1
				'||v_valid_data_filter||'
				'||v_valid_batches_filter||' 		
				'||v_online_load_id_filter||'
				'||V_QUALIFY||' 
			) with data and stats primary index ('||V_KEY_COLs||') on commit preserve rows;';
			
			SET v_create_delta =  'CREATE VOLATILE TABLE '||v_delta_TBL_NAME||' AS 
						(	
							with 
							tgt_matched_keys as
							(
								SELECT '||v_delta_COLS||' 
								FROM '||V_TGT_DB||'.'||i_TABLE_NAME||' src
								where exists (
											select 1 
											from '||v_table_name_1batch||' tgt 
											where '||v_delta_keys_eql||'
										)
							)
							,delta as
							(
							    SELECT '||v_delta_COLS||' 
								FROM '||v_table_name_1batch||'
							    MINUS
							    SELECT '||v_delta_COLS||' 
								FROM tgt_matched_keys
							)
							select * from '||v_table_name_1batch||' src
							where exists (
											select 1 
											from delta tgt 
											where '||v_delta_keys_eql||'
										)
						)
					  with data and stats primary index ('||V_KEY_COLs||') on commit preserve rows;';
			
			if v_large_object_count > 0
			then
				set V_SRC_TABLE = v_table_name_1batch;
			else
				set V_SRC_TABLE = v_delta_TBL_NAME;
			end if;
			
			set v_update_tgt = 
							'update tgt
							from '||V_TGT_DB||'.'||i_TABLE_NAME||' tgt, '||V_SRC_TABLE||' src
							set '||v_set_non_key_cols||'
							,UPD_DTTM = current_timestamp
							where '||v_keys_eql||';		
							';
		
            set v_insert_into_tgt = 
							'insert into '||V_TGT_DB||'.'||i_TABLE_NAME||'
							('||V_ALL_COLS||', INS_DTTM)
							SELECT 
								 '||V_ALL_COLS||'
								, current_timestamp INS_DTTM 
							FROM '||V_SRC_TABLE||' src
							where not exists (
											select 1 
											from '||V_TGT_DB||'.'||i_TABLE_NAME||' tgt 
											where '||v_keys_eql||'
											);
							';		
							

			CALL GDEV1_ETL.DBC_SYSEXECSQL_WITH_LOG(v_drop_1batch,V_SQL_SCRIPT_ID,v_run_id,V_SOURCE_NAME,i_TABLE_NAME,I_LOAD_ID,1/*1 log or 0 don't*/,V_DBC_RETURN_CODE,V_DBC_RETURN_MSG,V_DBC_ROWS_COUNT,v_dbc_run_id);
			
			SET V_SQL_SCRIPT_ID = V_SQL_SCRIPT_ID + 1;
			CALL GDEV1_ETL.DBC_SYSEXECSQL_WITH_LOG(v_drop_delta,V_SQL_SCRIPT_ID,v_run_id,V_SOURCE_NAME,i_TABLE_NAME,I_LOAD_ID,1/*1 log or 0 don't*/,V_DBC_RETURN_CODE,V_DBC_RETURN_MSG,V_DBC_ROWS_COUNT,v_dbc_run_id);

			
			SET V_SQL_SCRIPT_ID = V_SQL_SCRIPT_ID + 1;
			CALL GDEV1_ETL.DBC_SYSEXECSQL_WITH_LOG(v_create_1batch,V_SQL_SCRIPT_ID,v_run_id,V_SOURCE_NAME,i_TABLE_NAME,I_LOAD_ID,1/*1 log or 0 don't*/,V_DBC_RETURN_CODE,V_DBC_RETURN_MSG,V_DBC_ROWS_COUNT,v_dbc_run_id);
			IF V_DBC_RETURN_CODE <> 0 THEN
				SET V_RETURN_CODE = V_DBC_RETURN_CODE;
				SET V_RETURN_MSG = V_DBC_RETURN_MSG;
				LEAVE MainBlock;
			END IF;
			
			CALL GDEV1_ETL.COUNT_ROWS('', v_table_name_1batch, '',V_ROWS_IN_1batch, V_DBC_RETURN_CODE, V_DBC_RETURN_MSG);
			IF V_DBC_RETURN_CODE <> 0 THEN
				SET V_RETURN_CODE = V_DBC_RETURN_CODE;
				SET V_RETURN_MSG = V_DBC_RETURN_MSG;
				LEAVE MainBlock;
			END IF;
			
			if V_ROWS_IN_1batch = 0
			then
				LEAVE MainBlock;
			end if;
			
			CALL GDEV1_ETL.CHECK_MANDATORY_COLUMNS(NULL, v_table_name_1batch, V_KEY_COLs,V_NULLABLE_COLUMN, V_DBC_RETURN_CODE, V_DBC_RETURN_MSG);
			IF V_DBC_RETURN_CODE <> 0 THEN
				SET V_RETURN_CODE = V_DBC_RETURN_CODE;
				SET V_RETURN_MSG = V_DBC_RETURN_MSG;
				LEAVE MainBlock;
			END IF;
			
			SET V_SQL_SCRIPT_ID = V_SQL_SCRIPT_ID + 1;
			CALL GDEV1_ETL.DBC_SYSEXECSQL_WITH_LOG(v_create_delta,V_SQL_SCRIPT_ID,v_run_id,V_SOURCE_NAME,i_TABLE_NAME,I_LOAD_ID,1/*1 log or 0 don't*/,V_DBC_RETURN_CODE,V_DBC_RETURN_MSG,V_DBC_ROWS_COUNT,v_dbc_run_id);
			IF V_DBC_RETURN_CODE <> 0 THEN
				SET V_RETURN_CODE = V_DBC_RETURN_CODE;
				SET V_RETURN_MSG = V_DBC_RETURN_MSG;
				LEAVE MainBlock;
			END IF;
			
			CALL GDEV1_ETL.COUNT_ROWS('', v_delta_TBL_NAME, '',V_ROWS_IN_delta, V_DBC_RETURN_CODE, V_DBC_RETURN_MSG);
			IF V_DBC_RETURN_CODE <> 0 THEN
				SET V_RETURN_CODE = V_DBC_RETURN_CODE;
				SET V_RETURN_MSG = V_DBC_RETURN_MSG;
				LEAVE MainBlock;
			END IF;
			
			if V_ROWS_IN_delta = 0
			then
				LEAVE MainBlock;
			end if;
			
			SET V_SQL_SCRIPT_ID = V_SQL_SCRIPT_ID + 1;
			CALL GDEV1_ETL.DBC_SYSEXECSQL_WITH_LOG('BT',V_SQL_SCRIPT_ID,v_run_id,V_SOURCE_NAME,i_TABLE_NAME,I_LOAD_ID,1/*1 log or 0 don't*/,V_DBC_RETURN_CODE,V_DBC_RETURN_MSG,V_DBC_ROWS_COUNT,v_dbc_run_id);
			IF V_DBC_RETURN_CODE <> 0 THEN
				SET V_RETURN_CODE = V_DBC_RETURN_CODE;
				SET V_RETURN_MSG = V_DBC_RETURN_MSG;
				LEAVE MainBlock;
			END IF;	
			
			if V_UPDATE_ALL_TO_D <> ''
			then
				SET V_SQL_SCRIPT_ID = V_SQL_SCRIPT_ID + 1;
				CALL GDEV1_ETL.DBC_SYSEXECSQL_WITH_LOG(V_UPDATE_ALL_TO_D,V_SQL_SCRIPT_ID,v_run_id,V_SOURCE_NAME,i_TABLE_NAME,I_LOAD_ID,1/*1 log or 0 don't*/,V_DBC_RETURN_CODE,V_DBC_RETURN_MSG,V_DBC_ROWS_COUNT,v_dbc_run_id);
				IF V_DBC_RETURN_CODE <> 0 THEN
					SET V_RETURN_CODE = V_DBC_RETURN_CODE;
					SET V_RETURN_MSG = V_DBC_RETURN_MSG;
					LEAVE MainBlock;
				END IF;
			end if;
			
			if cast(v_update_tgt as char(10)) <> ''
			then
				SET V_SQL_SCRIPT_ID = V_SQL_SCRIPT_ID + 1;
				CALL GDEV1_ETL.DBC_SYSEXECSQL_WITH_LOG(v_update_tgt,V_SQL_SCRIPT_ID,v_run_id,V_SOURCE_NAME,i_TABLE_NAME,I_LOAD_ID,1/*1 log or 0 don't*/,V_DBC_RETURN_CODE,V_DBC_RETURN_MSG,V_DBC_ROWS_COUNT,v_dbc_run_id);
				IF V_DBC_RETURN_CODE <> 0 THEN
					SET V_RETURN_CODE = V_DBC_RETURN_CODE;
					SET V_RETURN_MSG = V_DBC_RETURN_MSG;
					LEAVE MainBlock;
				END IF;
				set V_UPDATED_ROWS_COUNT = V_UPDATED_ROWS_COUNT + V_DBC_ROWS_COUNT;
			end if;
			
			if cast(v_insert_into_tgt as char(10)) <> ''
			then
				SET V_SQL_SCRIPT_ID = V_SQL_SCRIPT_ID + 1;
				CALL GDEV1_ETL.DBC_SYSEXECSQL_WITH_LOG(v_insert_into_tgt,V_SQL_SCRIPT_ID,v_run_id,V_SOURCE_NAME,i_TABLE_NAME,I_LOAD_ID,1/*1 log or 0 don't*/,V_DBC_RETURN_CODE,V_DBC_RETURN_MSG,V_DBC_ROWS_COUNT,v_dbc_run_id);
				IF V_DBC_RETURN_CODE <> 0 THEN
					SET V_RETURN_CODE = V_DBC_RETURN_CODE;
					SET V_RETURN_MSG = V_DBC_RETURN_MSG;
					LEAVE MainBlock;
				END IF;
				set V_INSERTED_ROWS_COUNT = V_INSERTED_ROWS_COUNT + V_DBC_ROWS_COUNT;
			end if;
			
			SET V_SQL_SCRIPT_ID = V_SQL_SCRIPT_ID + 1;
			CALL GDEV1_ETL.DBC_SYSEXECSQL_WITH_LOG('ET',V_SQL_SCRIPT_ID,v_run_id,V_SOURCE_NAME,i_TABLE_NAME,I_LOAD_ID,1/*1 log or 0 don't*/,V_DBC_RETURN_CODE,V_DBC_RETURN_MSG,V_DBC_ROWS_COUNT,v_dbc_run_id);
			IF V_DBC_RETURN_CODE <> 0 THEN
				SET V_RETURN_CODE = V_DBC_RETURN_CODE;
				SET V_RETURN_MSG = V_DBC_RETURN_MSG;
				LEAVE MainBlock;
			END IF;
        END;
        
        INSERT INTO GDEV1_ETL.EXEC_PROCESS_LOGS  
        (RUN_ID, LOAD_ID, SOURCE_NAME, PROCESS_NAME, START_TIMESTAMP, END_TIMESTAMP, ROWS_INSERTED_COUNT, ROWS_UPDATED_COUNT, ROWS_DELETED_COUNT, ERROR_CODE, ERROR_MSG)  
        VALUES
        (v_run_id, i_LOAD_ID, V_SOURCE_NAME, i_TABLE_NAME, v_START_TIMESTAMP, current_timestamp, V_INSERTED_ROWS_COUNT, V_UPDATED_ROWS_COUNT, V_DELETED_ROWS_COUNT, V_RETURN_CODE, V_RETURN_MSG);
        
                                                                                         
        SET O_RETURN_CODE = V_RETURN_CODE;
        SET O_RETURN_MSG = V_RETURN_MSG;
        SET O_ROWS_INSERTED_COUNT	= V_INSERTED_ROWS_COUNT;
    	SET	O_ROWS_UPDATED_COUNT	= V_UPDATED_ROWS_COUNT;
    	SET	O_ROWS_DELETED_COUNT	= V_DELETED_ROWS_COUNT;
    	set o_run_id				= v_run_id;
    END;