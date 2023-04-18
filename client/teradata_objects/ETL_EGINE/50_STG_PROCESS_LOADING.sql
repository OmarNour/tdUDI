REPLACE  PROCEDURE /*VER.01*/ GDEV1P_PP.STG_PROCESS_LOADING
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
        			V_ROWS_IN_1batch, V_ROWS_IN_DELTA, V_ROWS_COUNT				FLOAT DEFAULT 0;
        DECLARE		v_run_id					BIGINT;
        DECLARE 	V_SQL_SCRIPT_ID				INTEGER DEFAULT 0;
        DECLARE 	V_DBC_RETURN_CODE 			INTEGER;
		DECLARE 	V_DBC_RETURN_MSG  			VARCHAR(1000);
		DECLARE 	V_DBC_ROWS_COUNT			FLOAT;
    	DECLARE 	v_drop_1batch
    				, v_update_tgt
    				, v_insert_into_tgt
    				, v_create_1batch			CLOB;    
    				
        DECLARE    	V_SRC_DB					VARCHAR(500);
        DECLARE    	V_SRC_TABLE					VARCHAR(500);
        DECLARE    	V_TGT_DB					VARCHAR(500);
        DECLARE    	V_TGT_TABLE					VARCHAR(500);
        DECLARE    	V_APPLY_TYPE				VARCHAR(500);
        DECLARE 	V_NULLABLE_COLUMN			VARCHAR(200);
        
        DECLARE		V_BATCH_ID					VARCHAR(150)	DEFAULT 'BATCH_ID';
        DECLARE	   	V_REF_KEY   				VARCHAR(150)	DEFAULT 'REF_KEY';
        DECLARE	   	V_MODIFICATION_TYPE 		VARCHAR(150)	DEFAULT 'MODIFICATION_TYPE';
        DECLARE		V_STAGING_DB				VARCHAR(150)	DEFAULT 'GDEV1T_STG';
     	
        DECLARE 	V_WITH_DATA_PI				VARCHAR(100) DEFAULT 'WITH DATA AND STATS PRIMARY INDEX';
		DECLARE 	V_WITH_DATA_UNIQUE_PI		VARCHAR(100) DEFAULT 'WITH DATA AND STATS UNIQUE PRIMARY INDEX';
	
		DECLARE		v_table_name_1batch,
					V_SOURCE_NAME,
				    V_LOADING_MODE,
				    V_REJECTION_TABLE_NAME,
				    V_BUSINESS_RULES_TABLE_NAME,
				    V_SOURCE_DB					VARCHAR(500);
				    
		DECLARE		V_IS_TARANSACTIOANL			INTEGER;
		declare		v_online_load_id_filter,
					v_valid_batches_filter,
					V_UPDATE_ALL_TO_D,V_ALL_COLS,
					V_KEY_COLs, v_, v_keys_eql,
					v_set_non_key_cols, v_valid_data_filter			varchar(2000) default '';
		
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
		    SRC.LOADING_MODE,
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
		    V_LOADING_MODE,
		    V_REJECTION_TABLE_NAME,
		    V_BUSINESS_RULES_TABLE_NAME,
		    V_SOURCE_DB,
		    V_IS_TARANSACTIOANL;
            
		    IF V_SOURCE_NAME IS NULL
            THEN
            	set V_RETURN_CODE = -1;
            	SET V_RETURN_MSG = 'Invalid Proccess!';
            	leave MAINBLOCK;
            end if;
            
            SELECT 
			 TRIM( TRAILING  ',' FROM (XMLAGG(trim(Key_Column)|| ',' ORDER BY Key_Column) (VARCHAR(10000)))) Key_Column
			, trim(XMLAGG( 'TGT.' || trim(Key_Column)|| ' = SRC.' ||trim(Key_Column) || ' AND ' ORDER BY Key_Column) (VARCHAR(10000))) _keys_eql
			, left(_keys_eql,(length(_keys_eql)-3)(int)) Keys_eql
			from GDEV1t_GCFR.TRANSFORM_KEYCOL
			WHERE DB_NAME = V_STAGING_DB
			AND TABLE_NAME  = i_TABLE_NAME
			INTO V_KEY_COLs, v_, v_keys_eql;
			
			if coalesce(V_KEY_COLs,'') = ''
			then
				set V_RETURN_CODE = -1;
            	SET V_RETURN_MSG = 'No keys defined for table '||i_TABLE_NAME||'!';
            	leave MAINBLOCK;
			end if;
			
			SELECT 
			TRIM( TRAILING  ',' FROM (XMLAGG(trim(COLUMNNAME)|| ' = SRC.' ||trim(COLUMNNAME) || ',' ORDER BY COLUMNNAME) (VARCHAR(10000)))) SET_COLS
			FROM DBC.COLUMNSV v
			WHERE DATABASENAME = V_STAGING_DB
			AND TABLENAME  = i_TABLE_NAME
			and COLUMNNAME not in ('INS_DTTM' ,'UPD_DTTM')
			and not exists (select 1 
							from GDEV1t_GCFR.TRANSFORM_KEYCOL k 
							where k.TABLE_NAME = v.DATABASENAME 
							and k.TABLE_NAME = v.TABLENAME
							and k.Key_Column = v.COLUMNNAME)
			GROUP BY DATABASENAME, TABLENAME
			INTO v_set_non_key_cols;
			
			SELECT 
			TRIM( TRAILING  ',' FROM (XMLAGG(trim(COLUMNNAME)|| ',' ORDER BY COLUMNNAME) (VARCHAR(10000)))) COLS
			FROM DBC.COLUMNSV v
			WHERE DATABASENAME = V_STAGING_DB
			AND TABLENAME  = i_TABLE_NAME
			and COLUMNNAME not in ('INS_DTTM' ,'UPD_DTTM')
			GROUP BY DATABASENAME, TABLENAME
			INTO V_ALL_COLS;
			
		    set v_table_name_1batch = i_TABLE_NAME||'_1batch';
		    if V_REJECTION_TABLE_NAME is not null and V_BUSINESS_RULES_TABLE_NAME is not null
		    then
			    set v_valid_data_filter = 
					'and not exists
						(
							sel 1 
							from '||V_SOURCE_DB||'.'||V_REJECTION_TABLE_NAME||' r  
							where r.REF_KEY  = x.REF_KEY
							and exists (
											select 1 
											from '||V_SOURCE_DB||'.'||V_BUSINESS_RULES_TABLE_NAME||' br 
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
				
				if i_LOAD_ID = 'RELOAD_DONE'
				then
					set v_online_load_id_filter = ' and exists (
																select 1 
																from GDEV1t_GCFR.ONLINE_RUN_CONFIG c 
																where c.LOAD_ID = x.LOAD_ID 
																and c.STATUS <> ''DELETED''
																and c.STG_status = ''DONE''
																)';		
				end if;
				
				set v_valid_batches_filter = 
						' and exists 
						(
							SELECT 1
							FROM '||V_SOURCE_DB||'.ibm_audit_table adt
							WHERE adt.DB2_TO_TERA_STATUS in (''SUCCESS'', ''SUCCESS WITH WARNINGS'')
							AND adt.DB2_JOB_END_TIME IS NOT NULL  
							AND adt.MIC_EXECUTIONGUID = x.LOAD_ID
							AND adt.MIC_RUNID = x.BATCH_ID
							AND exists (
											select 1 
											from GDEV1t_GCFR.SOURCE_NAME_LKP lkp
											where lkp.teradata_SOURCE_NAME = '''||V_SOURCE_DB||'''
											and lkp.MICROSOFT_SOURCE_NAME = adt.MIC_GANAME
										)
						)';	
					
            else
            	SET V_UPDATE_ALL_TO_D = 'update '||V_STAGING_DB||'.'||i_TABLE_NAME||' set modification_type = ''D''; ';
            end if;
            
			
            set v_drop_1batch = 'DROP TABLE '||v_table_name_1batch||';';
            set v_create_1batch = 
			'create multiset volatile TABLE '||v_table_name_1batch||'  , no fallback as 
			(
				sel * from '||V_SOURCE_DB||'.'||i_TABLE_NAME||' x
				where 1=1
				'||v_valid_data_filter||'
				'||v_valid_batches_filter||' 		
				'||v_online_load_id_filter||' 
				QUALIFY ROW_NUMBER()  OVER(PARTITION BY '||V_KEY_COLs||' ORDER BY BATCH_ID DESC,MODIFICATION_TYPE DESC,REF_KEY DESC)=1
			) with data and stats primary index ('||V_KEY_COLs||') on commit preserve rows;';
			
			set v_update_tgt = 
							'update tgt
							from '||V_STAGING_DB||'.'||i_TABLE_NAME||' tgt, '||v_table_name_1batch||' src
							set '||v_set_non_key_cols||'
							,UPD_DTTM = current_timestamp
							where '||v_keys_eql||';		
							';
		
            set v_insert_into_tgt = 
							'insert into '||V_STAGING_DB||'.'||i_TABLE_NAME||'
							('||V_ALL_COLS||', INS_DTTM)
							SELECT 
								 '||V_ALL_COLS||'
								, current_timestamp INS_DTTM 
							FROM '||v_table_name_1batch||' src
							where not exists (
											select 1 
											from '||V_STAGING_DB||'.'||i_TABLE_NAME||' tgt 
											where '||v_keys_eql||'
											);
							';		
							

			CALL GDEV1P_PP.DBC_SYSEXECSQL_WITH_LOG(v_drop_1batch,V_SQL_SCRIPT_ID,v_run_id,V_SOURCE_NAME,i_TABLE_NAME,I_LOAD_ID,1/*1 log or 0 don't*/,V_DBC_RETURN_CODE,V_DBC_RETURN_MSG,V_DBC_ROWS_COUNT);

			
			SET V_SQL_SCRIPT_ID = V_SQL_SCRIPT_ID + 1;
			CALL GDEV1P_PP.DBC_SYSEXECSQL_WITH_LOG(v_create_1batch,V_SQL_SCRIPT_ID,v_run_id,V_SOURCE_NAME,i_TABLE_NAME,I_LOAD_ID,1/*1 log or 0 don't*/,V_DBC_RETURN_CODE,V_DBC_RETURN_MSG,V_DBC_ROWS_COUNT);
			IF V_DBC_RETURN_CODE <> 0 THEN
				SET V_RETURN_CODE = V_DBC_RETURN_CODE;
				SET V_RETURN_MSG = V_DBC_RETURN_MSG;
				LEAVE MainBlock;
			END IF;
			
			CALL GDEV1P_PP.COUNT_ROWS('', v_table_name_1batch, '',V_ROWS_IN_1batch, V_DBC_RETURN_CODE, V_DBC_RETURN_MSG);
			IF V_DBC_RETURN_CODE <> 0 THEN
				SET V_RETURN_CODE = V_DBC_RETURN_CODE;
				SET V_RETURN_MSG = V_DBC_RETURN_MSG;
				LEAVE MainBlock;
			END IF;
			
			if V_ROWS_IN_1batch = 0
			then
				LEAVE MainBlock;
			end if;
			
			CALL GDEV1P_PP.CHECK_MANDATORY_COLUMNS(NULL, v_table_name_1batch, V_KEY_COLs,V_NULLABLE_COLUMN, V_DBC_RETURN_CODE, V_DBC_RETURN_MSG);
			IF V_DBC_RETURN_CODE <> 0 THEN
				SET V_RETURN_CODE = V_DBC_RETURN_CODE;
				SET V_RETURN_MSG = V_DBC_RETURN_MSG;
				LEAVE MainBlock;
			END IF;
			
			SET V_SQL_SCRIPT_ID = V_SQL_SCRIPT_ID + 1;
			CALL GDEV1P_PP.DBC_SYSEXECSQL_WITH_LOG('BT',V_SQL_SCRIPT_ID,v_run_id,V_SOURCE_NAME,i_TABLE_NAME,I_LOAD_ID,1/*1 log or 0 don't*/,V_DBC_RETURN_CODE,V_DBC_RETURN_MSG,V_DBC_ROWS_COUNT);
			IF V_DBC_RETURN_CODE <> 0 THEN
				SET V_RETURN_CODE = V_DBC_RETURN_CODE;
				SET V_RETURN_MSG = V_DBC_RETURN_MSG;
				LEAVE MainBlock;
			END IF;	
			
			if V_UPDATE_ALL_TO_D <> ''
			then
				SET V_SQL_SCRIPT_ID = V_SQL_SCRIPT_ID + 1;
				CALL GDEV1P_PP.DBC_SYSEXECSQL_WITH_LOG(V_UPDATE_ALL_TO_D,V_SQL_SCRIPT_ID,v_run_id,V_SOURCE_NAME,i_TABLE_NAME,I_LOAD_ID,1/*1 log or 0 don't*/,V_DBC_RETURN_CODE,V_DBC_RETURN_MSG,V_DBC_ROWS_COUNT);
				IF V_DBC_RETURN_CODE <> 0 THEN
					SET V_RETURN_CODE = V_DBC_RETURN_CODE;
					SET V_RETURN_MSG = V_DBC_RETURN_MSG;
					LEAVE MainBlock;
				END IF;
			end if;
			
			if cast(v_update_tgt as char(10)) <> ''
			then
				SET V_SQL_SCRIPT_ID = V_SQL_SCRIPT_ID + 1;
				CALL GDEV1P_PP.DBC_SYSEXECSQL_WITH_LOG(v_update_tgt,V_SQL_SCRIPT_ID,v_run_id,V_SOURCE_NAME,i_TABLE_NAME,I_LOAD_ID,1/*1 log or 0 don't*/,V_DBC_RETURN_CODE,V_DBC_RETURN_MSG,V_DBC_ROWS_COUNT);
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
				CALL GDEV1P_PP.DBC_SYSEXECSQL_WITH_LOG(v_insert_into_tgt,V_SQL_SCRIPT_ID,v_run_id,V_SOURCE_NAME,i_TABLE_NAME,I_LOAD_ID,1/*1 log or 0 don't*/,V_DBC_RETURN_CODE,V_DBC_RETURN_MSG,V_DBC_ROWS_COUNT);
				IF V_DBC_RETURN_CODE <> 0 THEN
					SET V_RETURN_CODE = V_DBC_RETURN_CODE;
					SET V_RETURN_MSG = V_DBC_RETURN_MSG;
					LEAVE MainBlock;
				END IF;
				set V_INSERTED_ROWS_COUNT = V_INSERTED_ROWS_COUNT + V_DBC_ROWS_COUNT;
			end if;
			
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
        SET O_ROWS_INSERTED_COUNT	= V_INSERTED_ROWS_COUNT;
    	SET	O_ROWS_UPDATED_COUNT	= V_UPDATED_ROWS_COUNT;
    	SET	O_ROWS_DELETED_COUNT	= V_DELETED_ROWS_COUNT;
    END;