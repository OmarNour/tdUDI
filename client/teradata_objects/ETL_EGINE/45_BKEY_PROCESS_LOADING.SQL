REPLACE PROCEDURE /*VER.2*/ GDEV1P_PP.BKEY_PROCESS_LOADING
(
	IN i_LOAD_ID 			VARCHAR(100), 
	IN i_run_id 			BIGINT,
	IN i_PROCESS_NAME  		VARCHAR(500), 
	OUT RETURN_CODE    		INTEGER,
	OUT RETURN_MSG  		VARCHAR(1000),
	OUT ROWS_COUNT_VALUE	INTEGER
)
/*
###################################################################################################################################################################
------------------------------------------------------------------------------------------------------------------------------------------------------------------
#  Version History
------------------------------------------------------------------------------------------------------------------------------------------------------------------
#  Version 		|	Date        		|	Author				|	COMMENT
#  1.0        	| 	13 DEC,  2022 		| 	Omar Nour			|	Initial
------------------------------------------------------------------------------------------------------------------------------------------------------------------

###################################################################################################################################################################
*/
------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Definition Of Procedure:
------------------------------------------------------------------------------------------------------------------------------------------------------------------


BEGIN

	DECLARE v_RETURN_CODE INTEGER;
	DECLARE v_RETURN_MSG VARCHAR(100);
	declare v_START_TIMESTAMP TIMESTAMP(6);

	DECLARE V_DBC_RETURN_CODE 			INTEGER;
	DECLARE V_DBC_RETURN_MSG  			VARCHAR(1000);
	DECLARE V_DBC_ROWS_COUNT			INTEGER;
	
	DECLARE v_Ctl_Id					INTEGER;
	DECLARE v_src_DB					VARCHAR(500);
	DECLARE v_src_table					VARCHAR(500);
	DECLARE v_tgt_db					VARCHAR(500);
	DECLARE v_tgt_table					VARCHAR(500);
	DECLARE v_Key_Set_Id				INTEGER;
	DECLARE v_Domain_Id					INTEGER;
	declare v_statment					CLOB;
	declare V_SOURCE_NAME 				VARCHAR(500);
	DECLARE C1 CURSOR FOR S1;

	DECLARE EXIT HANDLER FOR SQLEXCEPTION 
	BEGIN 
		SET v_RETURN_CODE = SQLCODE;
		GET DIAGNOSTICS EXCEPTION 1 v_RETURN_MSG = MESSAGE_TEXT; 
		SET RETURN_CODE = v_RETURN_CODE;
		SET RETURN_MSG = v_RETURN_MSG;		
	END;
	
		select 
			 P.SRC_DB
			,P.SRC_TABLE
			,P.TGT_DB  
			,P.TGT_TABLE               
			,P.Key_Set_Id
			,P.Domain_Id
			,p.source_name
		from GDEV1T_GCFR.PROCESS P
		where  P.PROCESS_NAME = I_PROCESS_NAME
		into v_src_DB
			,v_src_table
			,v_tgt_db
			,v_tgt_table
			,v_Key_Set_Id
			,v_Domain_Id
			,V_SOURCE_NAME;

	BEGIN
		-- Model Block			
		if v_Domain_Id is not null
		then
			set v_statment = '
						insert into '||v_tgt_db||'.'||v_tgt_table||'
						(source_key, domain_id, key_set_id, edw_key, start_date, process_name)

						with t_max_edw_key as
						(
							select coalesce(max(edw_key), 1000) max_edw_key 
							from  '||v_tgt_db||'.'||v_tgt_table||' 						
						)
						select src.source_key
						, '||v_Domain_Id||' domain_id
						, '||v_Key_Set_Id||' key_set_id					
						,sum(1) over (order by src.source_key ROWS UNBOUNDED PRECEDING) + MK.max_edw_key edw_key
						, current_date start_date
						, '''||i_PROCESS_NAME||''' process_name
						from '||v_src_DB||'.'||v_src_table||' src, t_max_edw_key MK
						where not exists (select 1 
										from '||v_tgt_db||'.'||v_tgt_table||'  tgt
										where src.source_key=tgt.source_key
										and tgt.domain_id = '||v_Domain_Id||'
										);
			';
			CALL GDEV1P_PP.DBC_SYSEXECSQL_WITH_LOG(v_statment,1,I_RUN_ID,V_SOURCE_NAME,i_PROCESS_NAME,I_LOAD_ID,1/*1 log or 0 don't*/,V_DBC_RETURN_CODE,V_DBC_RETURN_MSG,V_DBC_ROWS_COUNT);
		else
			set V_DBC_RETURN_CODE = -1;
			set V_DBC_RETURN_MSG = 'BKEY is not registered in PROCESS table';
		end if;
	end;
	
	IF V_DBC_RETURN_CODE = 0 THEN
		SET RETURN_CODE =  0;
		SET RETURN_MSG =  'Process Completed Successfully';
		SET ROWS_COUNT_VALUE = V_DBC_ROWS_COUNT;
	ELSE
		SET RETURN_CODE =  -1;
		SET RETURN_MSG =  V_DBC_RETURN_MSG;
		SET ROWS_COUNT_VALUE = 0;
	end if;
END;