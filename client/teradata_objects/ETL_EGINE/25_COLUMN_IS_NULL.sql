REPLACE PROCEDURE GDEV1_ETL.COLUMN_IS_NULL
/*
###################################################################################################################################################################
------------------------------------------------------------------------------------------------------------------------------------------------------------------
#  Version History
------------------------------------------------------------------------------------------------------------------------------------------------------------------
#  Version        | Date                    | Author        | COMMENT
#  1.0            | 28 December, 2021      | Omar Nour     | Initial version
------------------------------------------------------------------------------------------------------------------------------------------------------------------
#  Examples:
CALL GDEV1_ETL.COLUMN_IS_NULL('GDEV1V_base', 'PRTY', 'UPDATE_PROCESS_NAME', IS_NULL, RETURN_CODE, RETURN_MSG);
CALL GDEV1_ETL.COLUMN_IS_NULL('GDEV1V_base', 'PRTY', 'PROCESS_NAME', IS_NULL, RETURN_CODE, RETURN_MSG);
###################################################################################################################################################################
*/

(
    -- Input parameters
    IN      I_DATABASE_NAME     VARCHAR(300),   -- Name of the database
    IN      I_TABLE_NAME        VARCHAR(300),   -- Name of the table
    IN      I_COLUMN_NAME       VARCHAR(300),   -- Name of the column
    
    -- Output parameters
    OUT     O_IS_NULL           INTEGER,        -- Flag indicating if the column is null (1) or not null (0)
    OUT     O_RETURN_CODE       INTEGER,        -- Return code indicating the execution status
    OUT     O_RETURN_MSG        VARCHAR(10000)  -- Return message providing additional information
)

BEGIN
    
    DECLARE V_DATABASE_NAME VARCHAR(300) DEFAULT '';     -- Variable to store the formatted database name
    DECLARE V_SCRIPT        VARCHAR(10000) DEFAULT '';   -- Variable to store the SQL script
    DECLARE V_CURSOR_RESULT INTEGER;                     -- Variable to store the cursor result
    DECLARE V_CURSOR        CURSOR FOR V_STATEMENT;      -- Cursor declaration
    
    -- Exception handling for SQL exceptions
    DECLARE EXIT HANDLER FOR SQLEXCEPTION 
    BEGIN           
        SET O_RETURN_CODE = SQLCODE;
        GET DIAGNOSTICS EXCEPTION 1 O_RETURN_MSG = MESSAGE_TEXT;             
    END;
    
    -- Format the database name if provided
    IF COALESCE(TRIM(I_DATABASE_NAME), '') <> '' THEN
        SET V_DATABASE_NAME = I_DATABASE_NAME || '.';
    END IF;
    
    -- Construct the SQL script to check if the column is null
    SET V_SCRIPT = 'sel 1 where exists (sel 1 from ' || V_DATABASE_NAME || I_TABLE_NAME || ' where ' || I_COLUMN_NAME || ' is null)';
    
    -- Prepare and execute the SQL statement
    PREPARE V_STATEMENT FROM V_SCRIPT;
    OPEN V_CURSOR;
    FETCH V_CURSOR INTO V_CURSOR_RESULT;
    CLOSE V_CURSOR;
    
    -- Set the flag indicating if the column is null or not
    IF V_CURSOR_RESULT IS NULL THEN
        SET O_IS_NULL = 0;   -- Column is not null
    ELSE
        SET O_IS_NULL = 1;   -- Column is null
    END IF;
    
    SET O_RETURN_CODE = 0;   -- Successful execution
    SET O_RETURN_MSG = 'Process Completed Successfully';    
END;
