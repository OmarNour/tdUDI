REPLACE PROCEDURE GDEV1P_PP.DBC_SYSEXECSQL_WITH_LOG
/*
###################################################################################################################################################################
------------------------------------------------------------------------------------------------------------------------------------------------------------------
#  Version History
------------------------------------------------------------------------------------------------------------------------------------------------------------------
#  Version 		| Date        			| Author
#  1.0        	| 23 December, 2019 	| Omar Nour
------------------------------------------------------------------------------------------------------------------------------------------------------------------
#  Examples:
#  CALL GDEV1P_PP.DBC_SYSEXECSQL_WITH_LOG('sel 1 ;', 1, 99999, 'SCRIPT_SRC', 'SCRIPT_TBL', 'SCRIPT_LOAD_ID', 1, o_RETURN_CODE, o_RETURN_MSG, o_Rows_Count);
###################################################################################################################################################################
*/

(	IN 	i_SQL_SCRIPT 	clob character set unicode,
	IN 	i_SCRIPT_SEQ		integer,
	IN 	i_run_id 		BIGINT,
	in 	i_SOURCE_NAME	VARCHAR(50),
	in	i_TABLE_NAME	VARCHAR(500),
	in	i_LOAD_ID		VARCHAR(100),
	in	i_log			integer, -- 1 log , 0 dont log
	OUT o_RETURN_CODE    	INTEGER,
	OUT o_RETURN_MSG  	VARCHAR(10000),
	OUT o_Rows_Count 		INTEGER
)

BEGIN
	
	declare v_dbc_RETURN_CODE    	INTEGER default 0;
	declare v_dbc_RETURN_MSG  	VARCHAR(1000) DEFAULT 'Process Completed Successfully';
	declare v_dbc_Rows_Count		integer;
	DECLARE v_run_id BIGINT;
	DECLARE V_QUERY_BAND varchar(64000);
	declare v_START_TIMESTAMP TIMESTAMP(6);
	DECLARE EXIT HANDLER FOR SQLEXCEPTION 
	BEGIN 		
		
		SET v_dbc_RETURN_CODE = SQLCODE;
		GET DIAGNOSTICS EXCEPTION 1 v_dbc_RETURN_MSG = MESSAGE_TEXT; 			
	
		SET o_RETURN_MSG = v_dbc_RETURN_MSG;
		SET o_RETURN_CODE  = v_dbc_RETURN_CODE;
		SET o_Rows_Count 	= 0;

		
		if i_log = 1 then
			INSERT INTO GDEV1T_GCFR.EXEC_LOGS  (RUN_ID, LOAD_ID, SOURCE_NAME, TABLE_NAME, SQL_SCRIPT_SEQ, SQL_SCRIPT, START_TIMESTAMP, END_TIMESTAMP, ERROR_CODE, ERROR_MSG, ROWS_COUNT)  
			VALUES(COALESCE(v_run_id,i_run_id,''),i_LOAD_ID,i_SOURCE_NAME,i_TABLE_NAME,i_SCRIPT_SEQ,i_SQL_SCRIPT,v_START_TIMESTAMP,CURRENT_TIMESTAMP,v_dbc_RETURN_CODE, v_dbc_RETURN_MSG, 0) ;
		END IF;
		
		
	END;
	
	
	if i_log = 1 then
		IF i_run_id IS NULL THEN 
			--select cast(oreplace(oreplace(oreplace(oreplace(oreplace( cast(CURRENT_TIMESTAMP as varchar(38)),'-',''),' ',''),':',''),'.',''),'+','')as decimal(38))
			select GDEV1P_FF.generate_run_id()
			into v_run_id;
		ELSE 	
			SET v_run_id=	i_run_id;
		END IF;
	end if;
	
	SELECT 'PROCEDURE=GDEV1P_PP.DBC_SYSEXECSQL_WITH_LOG;' 
		|| 'i_SCRIPT_SEQ='||COALESCE(i_SCRIPT_SEQ,'')||';'
		|| 'i_run_id='||COALESCE(i_run_id,'')||';'
		|| 'i_SOURCE_NAME='||COALESCE(i_SOURCE_NAME,'')||';'
		|| 'i_TABLE_NAME='||COALESCE(i_TABLE_NAME,'')||';'
		|| 'i_LOAD_ID='||COALESCE(i_LOAD_ID,'')||';'
		|| 'i_log='||COALESCE(i_log,'')||';'
		into V_QUERY_BAND;
		
	set v_START_TIMESTAMP = CURRENT_TIMESTAMP;
	CALL GDEV1P_PP.DBC_SYSEXECSQL(i_SQL_SCRIPT, V_QUERY_BAND, v_dbc_RETURN_CODE, v_dbc_RETURN_MSG, v_dbc_Rows_Count);	
	
	
	if i_log = 1 then
		INSERT INTO GDEV1T_GCFR.EXEC_LOGS  (RUN_ID, LOAD_ID, SOURCE_NAME, TABLE_NAME, SQL_SCRIPT_SEQ, SQL_SCRIPT, START_TIMESTAMP, END_TIMESTAMP, ERROR_CODE, ERROR_MSG, ROWS_COUNT)  
		VALUES(v_run_id,i_LOAD_ID,i_SOURCE_NAME,i_TABLE_NAME,i_SCRIPT_SEQ,i_SQL_SCRIPT,v_START_TIMESTAMP,CURRENT_TIMESTAMP,v_dbc_RETURN_CODE, v_dbc_RETURN_MSG, v_dbc_Rows_Count) ;
	end if;
	
	SET o_RETURN_CODE =  v_dbc_RETURN_CODE;
	SET o_RETURN_MSG 	=  v_dbc_RETURN_MSG;
	SET o_Rows_Count 	= v_dbc_Rows_Count;
END;