SET SESSION DATABASE GDEV1_ETL;
/*

--DROP TABLE PARAMETERS;

DROP TABLE LAYERS;

DROP TABLE SOURCE_SYSTEMS;

DROP TABLE SOURCE_SYSTEM_TABLES;

DROP TABLE CORE_TABLES;

DROP TABLE TRANSFORM_KEYCOL;

DROP TABLE HISTORY;

DROP TABLE PROCESS;

DROP TABLE EXEC_SOURCE_LOGS;

DROP TABLE EXEC_PROCESS_LOGS;

DROP TABLE EXEC_SCRIPT_LOGS;

DROP TABLE CDC_AUDIT;
*/

--CREATE MULTISET TABLE PARAMETERS ,FALLBACK ,
--     NO BEFORE JOURNAL,
--     NO AFTER JOURNAL,
--     CHECKSUM = DEFAULT,
--     DEFAULT MERGEBLOCKRATIO,
--     MAP = TD_MAP1
--     (
--      p_key VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
--      p_value VARCHAR(500) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL
--	 )
--UNIQUE PRIMARY INDEX ( p_key );

CREATE MULTISET TABLE LAYERS ,FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      layer_name VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
      lyr_db VARCHAR(130) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
      src_db VARCHAR(130) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
      layer_Level INTEGER NOT NULL
	 )
UNIQUE PRIMARY INDEX ( layer_name );

CREATE MULTISET TABLE SOURCE_SYSTEMS ,FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      SOURCE_NAME VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
      LOADING_MODE VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
      REJECTION_TABLE_NAME VARCHAR(130) CHARACTER SET LATIN NOT CASESPECIFIC,
      BUSINESS_RULES_TABLE_NAME VARCHAR(130) CHARACTER SET LATIN NOT CASESPECIFIC,
      STG_ACTIVE INTEGER NOT NULL,
      BASE_ACTIVE INTEGER NOT NULL,
      IS_SCHEDULED INTEGER NOT NULL,
      SOURCE_LAYER VARCHAR(130) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
      DATA_SRC_CD INTEGER,
      ACTIVE INTEGER NOT NULL
      )
UNIQUE PRIMARY INDEX ( SOURCE_NAME );

CREATE MULTISET TABLE SOURCE_SYSTEM_TABLES ,FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      TABLE_NAME VARCHAR(130) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
      SOURCE_NAME VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
      IS_TARANSACTIOANL INTEGER NOT NULL,
      ACTIVE INTEGER NOT NULL
      )
UNIQUE PRIMARY INDEX ( TABLE_NAME );

CREATE MULTISET TABLE EDW_TABLES ,FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      LAYER_NAME VARCHAR(130) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
      TABLE_NAME VARCHAR(130) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
      IS_LOOKUP INTEGER NOT NULL,
      IS_HISTORY INTEGER NOT NULL,
      START_DATE_COLUMN VARCHAR(130) CHARACTER SET LATIN NOT CASESPECIFIC,
      END_DATE_COLUMN VARCHAR(130) CHARACTER SET LATIN NOT CASESPECIFIC,
      ACTIVE INTEGER NOT NULL
	 )
UNIQUE PRIMARY INDEX ( LAYER_NAME, TABLE_NAME );

CREATE MULTISET TABLE TRANSFORM_KEYCOL ,FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      LAYER_NAME VARCHAR(130) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
      TABLE_NAME VARCHAR(130) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
      KEY_COLUMN VARCHAR(130) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL
      )
UNIQUE PRIMARY INDEX ( LAYER_NAME ,TABLE_NAME ,KEY_COLUMN );

CREATE MULTISET TABLE PROCESS ,FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      PROCESS_NAME VARCHAR(130) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
      SOURCE_NAME VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
      TGT_LAYER VARCHAR(130) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
      TGT_TABLE VARCHAR(130) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
      SRC_TABLE VARCHAR(130) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
      APPLY_TYPE VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
      MAIN_TABLE_NAME VARCHAR(130) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
      KEY_SET_ID INTEGER,
      CODE_SET_ID INTEGER,
      DOMAIN_ID INTEGER,
      ACTIVE INTEGER NOT NULL
      )
UNIQUE PRIMARY INDEX ( PROCESS_NAME );

CREATE MULTISET TABLE "HISTORY" ,FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      PROCESS_NAME VARCHAR(130) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
      HISTORY_COLUMN VARCHAR(130) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL
  	)
UNIQUE PRIMARY INDEX ( PROCESS_NAME ,HISTORY_COLUMN );



CREATE MULTISET TABLE EXEC_SCRIPT_LOGS ,FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      RUN_ID BIGINT NOT NULL,
      LOAD_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,
      SOURCE_NAME VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
      PROCESS_NAME VARCHAR(130) CHARACTER SET LATIN NOT CASESPECIFIC,
      SQL_SCRIPT_SEQ INTEGER NOT NULL,
      SQL_SCRIPT CLOB(2097088000) CHARACTER SET LATIN NOT NULL,
      START_TIMESTAMP TIMESTAMP(6) NOT NULL,
      END_TIMESTAMP TIMESTAMP(6) NOT NULL,
      ROWS_COUNT FLOAT,
      ERROR_CODE INTEGER,
      ERROR_MSG VARCHAR(1000) CHARACTER SET LATIN NOT CASESPECIFIC,
      CREATED_BY VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL DEFAULT USER 
	   )
PRIMARY INDEX ( RUN_ID );

CREATE MULTISET TABLE EXEC_PROCESS_LOGS ,FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      RUN_ID BIGINT NOT NULL,
      LOAD_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,
      SOURCE_NAME VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
      PROCESS_NAME VARCHAR(130) CHARACTER SET LATIN NOT CASESPECIFIC,
      START_TIMESTAMP TIMESTAMP(6) NOT NULL,
      END_TIMESTAMP TIMESTAMP(6) NOT NULL,
      ROWS_INSERTED_COUNT FLOAT,
      ROWS_UPDATED_COUNT FLOAT,
      ROWS_DELETED_COUNT FLOAT,
      ERROR_CODE INTEGER,
      ERROR_MSG VARCHAR(1000) CHARACTER SET LATIN NOT CASESPECIFIC,
      CREATED_BY VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL DEFAULT USER 
	  )
PRIMARY INDEX ( RUN_ID );

CREATE MULTISET TABLE EXEC_SOURCE_LOGS ,FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      RUN_ID BIGINT NOT NULL,
      LOAD_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,
      SOURCE_NAME VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
      START_TIMESTAMP TIMESTAMP(6) NOT NULL,
      END_TIMESTAMP TIMESTAMP(6) NOT NULL,
      STG_DONE	INTEGER DEFAULT 0,
      BKEY_DONE	INTEGER DEFAULT 0,
      SRCI_DONE	INTEGER DEFAULT 0,
      CORE_DONE	INTEGER DEFAULT 0,
      DONE		INTEGER DEFAULT 0,
      DELETED	INTEGER DEFAULT 0,
      ROWS_INSERTED_COUNT FLOAT,
      ROWS_UPDATED_COUNT FLOAT,
      ROWS_DELETED_COUNT FLOAT,
      ERROR_CODE INTEGER,
      ERROR_MSG VARCHAR(1000) CHARACTER SET LATIN NOT CASESPECIFIC,
      CREATED_BY VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL DEFAULT USER 
      )
PRIMARY INDEX ( RUN_ID );

CREATE MULTISET TABLE CDC_AUDIT ,FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
     SOURCE_NAME VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,	
     LOAD_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
     BATCH_ID	INTEGER NOT NULL,
     TABLE_NAME VARCHAR(150) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
     PROCESSED	INTEGER NOT NULL DEFAULT 0,
     RECORDS_COUNT float NOT NULL,
	 CREATED_BY VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL DEFAULT USER,
	 CREATED_AT	TIMESTAMP DEFAULT current_timestamp,
	 UPDATED_BY VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
	 UPDATED_AT	TIMESTAMP
     )
PRIMARY INDEX ( LOAD_ID, SOURCE_NAME );

