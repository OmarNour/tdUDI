REPLACE  PROCEDURE /*VER.01*/ GDEV1_ETL.STG_LOADING
    (
    IN 		i_SOURCE_NAME  			VARCHAR(200),
    IN 		i_TABLE_NAME  			VARCHAR(200),
    IN 		I_RUN_ID 				BIGINT,
    IN		I_LOAD_ID				VARCHAR(500),
    IN		I_RELOAD				INTEGER, /* 1 RELOAD ALL LOADS WHICH LOADED SUCCESSFULLY BEFORE, 0 IGNORE*/
    IN		I_ALL_BATCHES			INTEGER, /* 1 INCLUDES BATCH_ID COLUMN AS A KEY, SO IT GRASPS ALL DATA FROM ALL BATCHES, 0 IGNORE*/
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
		        	V_dbc_INSERTED_ROWS_COUNT,V_dbc_UPDATED_ROWS_COUNT,V_dbc_DELETED_ROWS_COUNT 
        			FLOAT DEFAULT 0;
        DECLARE		v_run_id					BIGINT;
        
        DECLARE 	V_DBC_RETURN_CODE 			INTEGER;
		DECLARE 	V_DBC_RETURN_MSG  			VARCHAR(1000);
		DECLARE 	V_DBC_ROWS_COUNT			FLOAT;
		
        DECLARE EXIT HANDLER FOR SQLEXCEPTION
        BEGIN 	
            SET O_RETURN_CODE = SQLCODE;
            GET DIAGNOSTICS EXCEPTION 1 O_RETURN_MSG = MESSAGE_TEXT;
        END;
        
        
        MAINBLOCK:
        BEGIN
	        
	        IF i_run_id IS NULL THEN 
				select GDEV1_ETL.generate_run_id()
				into v_run_id;
			ELSE 	
				SET v_run_id=	i_run_id;
			END IF;
            
			FOR	loop1 AS loopx CURSOR FOR 
						SELECT 
					    TBL.TABLE_NAME
					    FROM GDEV1_ETL.STG_TABLES TBL
					    WHERE EXISTS (
					    SELECT 1
					        FROM GDEV1_ETL.SOURCE_SYSTEMS SRC
					        WHERE TBL.SOURCE_NAME=SRC.SOURCE_NAME
					            AND  SRC.ACTIVE = 1
					            AND  SRC.STG_ACTIVE = 1
					            AND  SRC.ACTIVE = 1
					    )
					        AND  TBL.ACTIVE = 1
					        AND  (TBL.TABLE_NAME = i_TABLE_NAME or i_TABLE_NAME is  null )
					        AND  (TBL.SOURCE_NAME = i_SOURCE_NAME or i_SOURCE_NAME is  null )
					        
			DO
				call GDEV1_ETL.STG_PROCESS_LOADING
											    (
											    loop1.TABLE_NAME	--IN 		I_PROCESSNAME  			VARCHAR(1000),
											    ,v_run_id			--IN 		I_RUN_ID 				BIGINT,
											    ,I_LOAD_ID			--IN		I_LOAD_ID				VARCHAR(500),
											    ,I_RELOAD			--IN 		I_RELOAD,	
											    ,I_ALL_BATCHES		--IN 		I_ALL_BATCHES,	
											    ,V_dbc_INSERTED_ROWS_COUNT 	--OUT  	O_ROWS_INSERTED_COUNT	FLOAT,
											    ,V_dbc_updated_ROWS_COUNT 	--OUT  	O_ROWS_UPDATED_COUNT	FLOAT,
											    ,V_dbc_deleted_ROWS_COUNT 	--OUT  	O_ROWS_DELETED_COUNT	FLOAT,
											    ,V_DBC_RETURN_CODE 	--OUT  	O_RETURN_CODE  			INTEGER,
											    ,V_DBC_RETURN_MSG 	--OUT  	O_RETURN_MSG 			VARCHAR(5000)
											    );
				if v_dbc_return_code <> 0 and V_RETURN_CODE = 0
				then
					set V_RETURN_CODE = v_dbc_return_code;
					set V_RETURN_MSG = V_DBC_RETURN_MSG;
				end if;
				
				set V_INSERTED_ROWS_COUNT = V_INSERTED_ROWS_COUNT + V_dbc_INSERTED_ROWS_COUNT;
				set V_updated_ROWS_COUNT = V_updated_ROWS_COUNT + V_dbc_updated_ROWS_COUNT;
				set V_deleted_ROWS_COUNT = V_deleted_ROWS_COUNT + V_dbc_deleted_ROWS_COUNT;
			end for;
        END;
                                                                                        
                                                                                         
        SET O_RETURN_CODE = V_RETURN_CODE;
        SET O_RETURN_MSG = V_RETURN_MSG;
        
        SET O_ROWS_INSERTED_COUNT	= V_INSERTED_ROWS_COUNT;
    	SET	O_ROWS_UPDATED_COUNT	= V_UPDATED_ROWS_COUNT;
    	SET	O_ROWS_DELETED_COUNT	= V_DELETED_ROWS_COUNT;
    END;