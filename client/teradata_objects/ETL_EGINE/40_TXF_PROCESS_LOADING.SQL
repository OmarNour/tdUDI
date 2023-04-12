REPLACE  PROCEDURE /*VER.01*/ GDEV1P_PP.TXF_PROCESS_LOADING
    (
    IN 		I_PROCESSNAME  			VARCHAR(1000),
    IN 		I_APPLY_TYPE  			VARCHAR(500),
    IN 		I_RUN_ID 				BIGINT,
    IN		I_LOAD_ID				VARCHAR(500),
    OUT  	O_ROWS_COUNT  			BIGINT,
    OUT  	O_RETURN_CODE  			INTEGER,
    OUT  	O_RETURN_MSG 			VARCHAR(5000)
    )
    BEGIN
        DECLARE 	V_RETURN_CODE  				INTEGER DEFAULT 0;
        DECLARE 	V_RETURN_MSG 				VARCHAR(5000) DEFAULT 'Process Completed Successfully';
        DECLARE 	V_ROWS_IN_DELTA, V_ROWS_COUNT				FLOAT DEFAULT 0;
        
        DECLARE 	V_SQL_SCRIPT_ID				INTEGER DEFAULT 0;
        DECLARE 	V_DBC_RETURN_CODE 			INTEGER;
		DECLARE 	V_DBC_RETURN_MSG  			VARCHAR(1000);
		DECLARE 	V_DBC_ROWS_COUNT			FLOAT;
                                                                
        DECLARE 	V_SOURCE_NAME 				VARCHAR(500);
        DECLARE    	V_SRC_DB					VARCHAR(500);
        DECLARE    	V_SRC_TABLE					VARCHAR(500);
        DECLARE    	V_TGT_DB					VARCHAR(500);
        DECLARE    	V_TGT_TABLE					VARCHAR(500);
        DECLARE    	V_APPLY_TYPE				VARCHAR(500);
        
        DECLARE		V_BATCH_ID					VARCHAR(150)	DEFAULT 'BATCH_ID';
        DECLARE	   	V_REF_KEY   				VARCHAR(150)	DEFAULT 'REF_KEY';
        DECLARE	   	V_MODIFICATION_TYPE 		VARCHAR(150)	DEFAULT 'MODIFICATION_TYPE';
        
        DECLARE		V_INSERT_TYPE				VARCHAR(150)	DEFAULT 'INSERT';
        DECLARE		V_UPSERT_TYPE				VARCHAR(150)	DEFAULT 'UPSERT';
        DECLARE		V_UPSERTDELETE_TYPE			VARCHAR(150)	DEFAULT 'UPSERTDELET';
        DECLARE		V_HISTROY_TYPE				VARCHAR(150)	DEFAULT 'HISTORY';
        
		DECLARE		V_KEY_COLUMNS				VARCHAR(2000);
        DECLARE		V_HISTORY_COLUMNS			VARCHAR(2000)	DEFAULT '';
     	DECLARE	   	V_START_DATE_1SEC 			VARCHAR(5000)	DEFAULT '';
     	DECLARE	   	V_START_DATE_COLUMN 		VARCHAR(150)	DEFAULT '';
     	DECLARE	   	V_QUALIFY_BY_KEY 			VARCHAR(5000)	DEFAULT '';
     	DECLARE		V_FULL_KEY					VARCHAR(2000)	DEFAULT '';
     	DECLARE		V_FULL_KEY_JOIN				VARCHAR(2000)	DEFAULT '';     	
        DECLARE		V_NORMAL_COMMON_COLUMNS		VARCHAR(2000)	DEFAULT '';
        DECLARE 	V_KEY_COLUMNS_JOIN			VARCHAR(2000)	DEFAULT '';
        DECLARE 	V_START_DATE_COLUMN_JOIN	VARCHAR(2000)	DEFAULT '';
        DECLARE		V_DELTA_INPUT_VIEW_NAME		VARCHAR(150)	DEFAULT '';
        
     	DECLARE 	V_VT_INPUT_VIEW, V_VT_DELTA_INPUT_VIEW, V_TMP_INPUT_VIEW		clob character set unicode;
     	DECLARE 	V_DROP_INPUT_VIEW, V_DROP_DELTA_INPUT_VIEW, V_DROP_TMP_INPUT_VIEW	VARCHAR(2000);
     	
        DECLARE 	V_WITH_DATA_PI			VARCHAR(100) DEFAULT 'WITH DATA AND STATS PRIMARY INDEX';
		DECLARE 	V_WITH_DATA_UNIQUE_PI	VARCHAR(100) DEFAULT 'WITH DATA AND STATS UNIQUE PRIMARY INDEX';
	
        DECLARE EXIT HANDLER FOR SQLEXCEPTION
        BEGIN 	
            SET O_RETURN_CODE = SQLCODE;
            GET DIAGNOSTICS EXCEPTION 1 O_RETURN_MSG = MESSAGE_TEXT;
        END;
        
        MAINBLOCK:
        BEGIN
            SELECT 
            SOURCE_NAME,
            SRC_DB,
            SRC_TABLE,
            TGT_DB,
            TGT_TABLE,
            coalesce(I_APPLY_TYPE,APPLY_TYPE) APPLY_TYPE
            ,KEY_COLUMNS
            ,COALESCE(START_DATE_COLUMN,'') START_DATE_COLUMN
            ,COALESCE(HISTORY_COLUMNS,'') HISTORY_COLUMNS
            ,COALESCE(','||NORMAL_COMMON_COLUMNS,'') NORMAL_COMMON_COLUMNS
            ,KEY_COLUMNS_JOIN
            ,COALESCE('X.'||START_DATE_COLUMN||' = Y.'||START_DATE_COLUMN,'') START_DATE_COLUMN_JOIN
            FROM GDEV1V_GCFR.PROCESS
            WHERE PROCESS_NAME = I_PROCESSNAME
                AND  PROCESS_TYPE = 'TXF'
                AND  ACTIVE = 1
            INTO V_SOURCE_NAME, V_SRC_DB, V_SRC_TABLE , V_TGT_DB, V_TGT_TABLE, V_APPLY_TYPE
            	,V_KEY_COLUMNS ,V_START_DATE_COLUMN ,V_HISTORY_COLUMNS, V_NORMAL_COMMON_COLUMNS
            	,V_KEY_COLUMNS_JOIN, V_START_DATE_COLUMN_JOIN;
            
            IF V_SOURCE_NAME IS NULL
            THEN
            	set V_RETURN_CODE = -1;
            	SET V_RETURN_MSG = 'Invalid Proccess!';
            	leave MAINBLOCK;
            end if;
            
            SET V_FULL_KEY = V_KEY_COLUMNS;
            
            if V_APPLY_TYPE = V_HISTROY_TYPE
            then
            	SET V_FULL_KEY = V_FULL_KEY||', '||V_START_DATE_COLUMN;
            	SET V_FULL_KEY_JOIN = V_KEY_COLUMNS_JOIN||' AND '||V_START_DATE_COLUMN_JOIN;
            	
            	SET V_HISTORY_COLUMNS = ','||V_HISTORY_COLUMNS;
            	select 
					'/* ********************** START RN + 1 SEC ********************************** */
						, 	ROW_NUMBER () OVER (PARTITION BY '||V_FULL_KEY||' 
												ORDER BY  '||V_BATCH_ID||' ,'||V_MODIFICATION_TYPE||' ,'||V_REF_KEY||' ) - 1 AS RN
						, 	CAST(CAST('||V_START_DATE_COLUMN||' AS VARCHAR(19)) AS TIMESTAMP (0)) 
						+  	CAST((RN/3600) AS INTERVAL HOUR )
						+	CAST((RN/60)MOD 60 AS INTERVAL MINUTE ) 
						+	CAST((RN) MOD 60 AS INTERVAL SECOND ) AS '||V_START_DATE_COLUMN||'					 
					/* ********************** END RN + 1 SEC ********************************** */'
				into V_START_DATE_1SEC;
				
            end if;
           	
            SET V_QUALIFY_BY_KEY = 'QUALIFY ROW_NUMBER()  OVER(PARTITION BY '||V_FULL_KEY||' ORDER BY  '||V_BATCH_ID||' DESC, '||V_MODIFICATION_TYPE||' DESC, '||V_REF_KEY||' DESC)=1 ';
          	select 
          	 'DROP TABLE TMP_INPUT_VIEW_'		|| V_TGT_TABLE ||';'	V_DROP_TMP_INPUT_VIEW
          	,'CREATE VOLATILE MULTISET TABLE TMP_INPUT_VIEW_' || V_TGT_TABLE || ' , no fallback AS 
				(
					with inpt_view as
					(
						SELECT 
							 '||V_BATCH_ID  ||	',' || V_MODIFICATION_TYPE	|| ',' ||V_REF_KEY||'
							,'||V_KEY_COLUMNS		|| V_NORMAL_COMMON_COLUMNS 		
							  ||V_HISTORY_COLUMNS 	|| V_START_DATE_1SEC||'
							,CASE WHEN ' ||V_MODIFICATION_TYPE|| ' = ''D'' THEN 1 ELSE 0 END INPV_DELETE_FLAG
							from ' || V_SRC_DB || '.' || V_SRC_TABLE ||'
					)
					select * from inpt_view
					' || V_QUALIFY_BY_KEY || '						 
				)  ' || V_WITH_DATA_UNIQUE_PI || '  (' || V_FULL_KEY || ') ON COMMIT PRESERVE ROWS;' 
			into V_DROP_TMP_INPUT_VIEW, V_TMP_INPUT_VIEW;
			--------------------------------------------------------------------------------------------------------------------------------
			select 
				 'DELTA_INPUT_VIEW_'		|| V_TGT_TABLE  
				,'DROP TABLE DELTA_INPUT_VIEW_'		|| V_TGT_TABLE ||';'
				,'CREATE VOLATILE MULTISET TABLE DELTA_INPUT_VIEW_' || V_TGT_TABLE || ' , no fallback AS 
				(
					SELECT ' || V_FULL_KEY || V_NORMAL_COMMON_COLUMNS ||V_HISTORY_COLUMNS	|| ', INPV_DELETE_FLAG					 
					FROM TMP_INPUT_VIEW_' || V_TGT_TABLE || '
					MINUS
					SELECT ' || V_FULL_KEY || V_NORMAL_COMMON_COLUMNS ||V_HISTORY_COLUMNS	|| ',CASE WHEN END_TS IS NOT NULL THEN 1 ELSE 0 END DELETE_FLAG					 
					FROM '||V_TGT_DB||'.' || V_TGT_TABLE || ' X
					where exists (
								select 1 
								from TMP_INPUT_VIEW_' || V_TGT_TABLE || ' y 
								where ' || V_KEY_COLUMNS_JOIN || '
								)							
				)  ' || V_WITH_DATA_UNIQUE_PI || '  (' || V_FULL_KEY || ') ON COMMIT PRESERVE ROWS;' 
			into V_DELTA_INPUT_VIEW_NAME, V_DROP_DELTA_INPUT_VIEW, V_VT_DELTA_INPUT_VIEW
			WHERE V_APPLY_TYPE <> V_INSERT_TYPE;
			--------------------------------------------------------------------------------------------------------------------------------			
			select 
			 'DROP TABLE INPUT_VIEW_' 			|| V_TGT_TABLE ||';'	
			,'CREATE VOLATILE MULTISET TABLE INPUT_VIEW_' || V_TGT_TABLE || ' , no fallback AS 
						(
							SELECT *					 
							FROM TMP_INPUT_VIEW_' || V_TGT_TABLE || ' x
							where exists (
										select 1 
										from DELTA_INPUT_VIEW_' || V_TGT_TABLE || ' y 
										where ' || V_FULL_KEY_JOIN || '
										)
						)  ' || V_WITH_DATA_UNIQUE_PI || '  (' || V_FULL_KEY || ') ON COMMIT PRESERVE ROWS;'
			into V_DROP_INPUT_VIEW, V_VT_INPUT_VIEW
			WHERE V_APPLY_TYPE <> V_INSERT_TYPE;
			--------------------------------------------------------------------------------------------------------------------------------
			--------------------------------------------------------------------------------------------------------------------------------
			--------------------------------------------------------------------------------------------------------------------------------
			CALL GDEV1P_PP.DBC_SYSEXECSQL_WITH_LOG(V_DROP_TMP_INPUT_VIEW,V_SQL_SCRIPT_ID,I_RUN_ID,V_SOURCE_NAME,I_PROCESSNAME,I_LOAD_ID,0/*1 log or 0 don't*/,V_DBC_RETURN_CODE,V_DBC_RETURN_MSG,V_DBC_ROWS_COUNT);
			CALL GDEV1P_PP.DBC_SYSEXECSQL_WITH_LOG(V_DROP_DELTA_INPUT_VIEW,V_SQL_SCRIPT_ID,I_RUN_ID,V_SOURCE_NAME,I_PROCESSNAME,I_LOAD_ID,0/*1 log or 0 don't*/,V_DBC_RETURN_CODE,V_DBC_RETURN_MSG,V_DBC_ROWS_COUNT);
			CALL GDEV1P_PP.DBC_SYSEXECSQL_WITH_LOG(V_DROP_INPUT_VIEW,V_SQL_SCRIPT_ID,I_RUN_ID,V_SOURCE_NAME,I_PROCESSNAME,I_LOAD_ID,0/*1 log or 0 don't*/,V_DBC_RETURN_CODE,V_DBC_RETURN_MSG,V_DBC_ROWS_COUNT);
			
			IF CAST(V_TMP_INPUT_VIEW AS CHAR(10)) <> ''
			THEN
				SET V_SQL_SCRIPT_ID = V_SQL_SCRIPT_ID + 1;
				CALL GDEV1P_PP.DBC_SYSEXECSQL_WITH_LOG(V_TMP_INPUT_VIEW,V_SQL_SCRIPT_ID,I_RUN_ID,V_SOURCE_NAME,I_PROCESSNAME,I_LOAD_ID,1/*1 log or 0 don't*/,V_DBC_RETURN_CODE,V_DBC_RETURN_MSG,V_DBC_ROWS_COUNT);
				IF V_DBC_RETURN_CODE <> 0 THEN
					SET V_RETURN_CODE = V_DBC_RETURN_CODE;
					SET V_RETURN_MSG = V_DBC_RETURN_MSG;
					LEAVE MainBlock;
				END IF;
			END IF;
			
			IF CAST(V_VT_DELTA_INPUT_VIEW AS CHAR(10)) <> ''
			THEN
				SET V_SQL_SCRIPT_ID = V_SQL_SCRIPT_ID + 1;
				CALL GDEV1P_PP.DBC_SYSEXECSQL_WITH_LOG(V_VT_DELTA_INPUT_VIEW,V_SQL_SCRIPT_ID,I_RUN_ID,V_SOURCE_NAME,I_PROCESSNAME,I_LOAD_ID,1/*1 log or 0 don't*/,V_DBC_RETURN_CODE,V_DBC_RETURN_MSG,V_DBC_ROWS_COUNT);
				IF V_DBC_RETURN_CODE <> 0 THEN
					SET V_RETURN_CODE = V_DBC_RETURN_CODE;
					SET V_RETURN_MSG = V_DBC_RETURN_MSG;
					LEAVE MainBlock;
				END IF;
				
				CALL GDEV1P_PP.COUNT_ROWS('', V_DELTA_INPUT_VIEW_NAME, V_ROWS_IN_DELTA, V_DBC_RETURN_CODE, V_DBC_RETURN_MSG);
				IF V_DBC_RETURN_CODE <> 0 THEN
					SET V_RETURN_CODE = V_DBC_RETURN_CODE;
					SET V_RETURN_MSG = V_DBC_RETURN_MSG;
					LEAVE MainBlock;
				END IF;
				
				if V_ROWS_IN_DELTA = 0
				then
					leave MainBlock;
				end if;
			
			END IF;
			
			IF CAST(V_VT_INPUT_VIEW AS CHAR(10)) <> ''
			THEN
				SET V_SQL_SCRIPT_ID = V_SQL_SCRIPT_ID + 1;
				CALL GDEV1P_PP.DBC_SYSEXECSQL_WITH_LOG(V_VT_INPUT_VIEW,V_SQL_SCRIPT_ID,I_RUN_ID,V_SOURCE_NAME,I_PROCESSNAME,I_LOAD_ID,1/*1 log or 0 don't*/,V_DBC_RETURN_CODE,V_DBC_RETURN_MSG,V_DBC_ROWS_COUNT);
				IF V_DBC_RETURN_CODE <> 0 THEN
					SET V_RETURN_CODE = V_DBC_RETURN_CODE;
					SET V_RETURN_MSG = V_DBC_RETURN_MSG;
					LEAVE MainBlock;
				END IF;
			END IF;
			
        END;
                                                                                        
                                                                                         
        SET O_RETURN_CODE = V_RETURN_CODE;
        SET O_RETURN_MSG = V_RETURN_MSG;
        SET O_ROWS_COUNT = V_ROWS_COUNT;
    END;