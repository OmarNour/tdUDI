REPLACE PROCEDURE GDEV1P_PP.DBC_SYSEXECSQL
    /*
    ###################################################################################################################################################################
    ------------------------------------------------------------------------------------------------------------------------------------------------------------------
    #  Version History
    ------------------------------------------------------------------------------------------------------------------------------------------------------------------
    #  Version 		| Date        			| Author
    #  1.0        	| 23 December, 2019 	| Omar Nour
    ------------------------------------------------------------------------------------------------------------------------------------------------------------------
    #  Examples:
    #  CALL GDEV1P_PP.DBC_SYSEXECSQL(''sel 1 ;, RETURN_CODE, RETURN_MSG);
    ###################################################################################################################################################################
    */
    
    (
    IN i_sql_script 		CLOB CHARACTER SET UNICODE,
    IN i_QUERY_BAND			VARCHAR(64000),
    OUT o_RETURN_CODE    	INTEGER,
    OUT o_RETURN_MSG  		VARCHAR(10000),
    OUT o_Rows_Count 		INTEGER
    )
    BEGIN
        	
           
        DECLARE EXIT HANDLER FOR SQLEXCEPTION
        BEGIN 		
            		
            SET o_RETURN_CODE = SQLCODE;
            GET DIAGNOSTICS EXCEPTION 1 o_RETURN_MSG = MESSAGE_TEXT;
        END;
        BEGIN TRANSACTION;
        SET QUERY_BAND = i_QUERY_BAND
        FOR TRANSACTION;	
        CALL DBC.SYSEXECSQL(i_sql_script);
        SET o_Rows_Count = ACTIVITY_COUNT;
        END TRANSACTION;	
        	
        
        SET o_RETURN_CODE =  0;
        SET o_RETURN_MSG =  'Process Completed Successfully';
    END;