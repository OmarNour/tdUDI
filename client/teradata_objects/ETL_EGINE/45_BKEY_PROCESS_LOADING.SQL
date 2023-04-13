REPLACE PROCEDURE /*VER.2*/ GDEV1P_PP.BKEY_PROCESS_LOADING
(
	IN i_LOAD_ID 			VARCHAR(100), 
	IN i_run_id 			DECIMAL(38),
	IN i_SOURCE_NAME 		VARCHAR(500),
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
	DECLARE v_In_DB_Name				VARCHAR(500);
	DECLARE v_In_Object_Name			VARCHAR(500);
	DECLARE v_Out_DB_Name				VARCHAR(500);
	DECLARE v_Out_Object_Name			VARCHAR(500);
	DECLARE v_Target_TableDatabaseName	VARCHAR(500);
	DECLARE v_Target_TableName			VARCHAR(500);
	DECLARE v_Key_Set_Id				INTEGER;
	DECLARE v_Domain_Id					INTEGER;
	declare v_statment					CLOB;
	DECLARE C1 CURSOR FOR S1;

	DECLARE EXIT HANDLER FOR SQLEXCEPTION 
	BEGIN 
		SET v_RETURN_CODE = SQLCODE;
		GET DIAGNOSTICS EXCEPTION 1 v_RETURN_MSG = MESSAGE_TEXT; 
		SET RETURN_CODE = v_RETURN_CODE;
		SET RETURN_MSG = v_RETURN_MSG;		
	END;
	
		select 
			 GCFR.Ctl_Id
			,GCFR.In_DB_Name
			,GCFR.In_Object_Name
			,GCFR.Out_DB_Name  
			,GCFR.Out_Object_Name               
			,GCFR.Target_TableDatabaseName      
			,GCFR.Target_TableName
			,GCFR.Key_Set_Id
			,GCFR.Domain_Id

		from GDEV1V_GCFR.GCFR_PROCESS GCFR
		where  GCFR.PROCESS_NAME = I_PROCESS_NAME
		into v_Ctl_Id
			,v_In_DB_Name
			,v_In_Object_Name
			,v_Out_DB_Name
			,v_Out_Object_Name
			,v_Target_TableDatabaseName
			,v_Target_TableName
			,v_Key_Set_Id
			,v_Domain_Id;

	BEGIN
		-- Model Block			
		if v_Domain_Id is not null
		then
			set v_statment = '
						insert into '||v_Target_TableDatabaseName||'.'||v_Target_TableName||'
						(source_key, domain_id, key_set_id, edw_key, start_date, ctl_id, process_name , process_id)

						with t_max_edw_key as
						(
							select coalesce(max(edw_key), 1000) max_edw_key 
							from  '||v_Out_DB_Name||'.'||v_Out_Object_Name||' 						
						)
						select src.source_key
						, '||v_Domain_Id||' domain_id
						, '||v_Key_Set_Id||' key_set_id					
						,sum(1) over (order by src.source_key ROWS UNBOUNDED PRECEDING) + MK.max_edw_key edw_key
						, current_date start_date
						, '||v_Ctl_Id||' ctl_id
						, '''||i_PROCESS_NAME||''' process_name, 1 process_id
						from '||v_In_DB_Name||'.'||v_In_Object_Name||' src, t_max_edw_key MK
						where not exists (select 1 
										from '||v_Out_DB_Name||'.'||v_Out_Object_Name||'  tgt
										where src.source_key=tgt.source_key
										and tgt.domain_id = '||v_Domain_Id||'
										);
			';
			CALL GDEV1P_PP.DBC_SYSEXECSQL_WITH_LOG(v_statment,1,I_RUN_ID,i_SOURCE_NAME,i_PROCESS_NAME,I_LOAD_ID,1/*1 log or 0 don't*/,V_DBC_RETURN_CODE,V_DBC_RETURN_MSG,V_DBC_ROWS_COUNT);
		else
			set V_DBC_RETURN_CODE = -1;
			set V_DBC_RETURN_MSG = 'BKEY is not registered in GPROD1V_GCFR.GCFR_PROCESS table';
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