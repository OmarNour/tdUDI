SET SESSION DATABASE GDEV1_ETL;

DROP TABLE CORE_TABLES;

DROP TABLE EXEC_LOGS;

DROP TABLE HISTORY;

DROP TABLE PROCESS;

DROP TABLE SOURCE_SYSTEMS;

DROP TABLE STG_TABLES;

DROP TABLE TRANSFORM_KEYCOL;

CREATE MULTISET TABLE CORE_TABLES ,FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      TABLE_NAME VARCHAR(130) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
      IS_LOOKUP INTEGER NOT NULL,
      IS_HISTORY INTEGER NOT NULL,
      START_DATE_COLUMN VARCHAR(130) CHARACTER SET LATIN NOT CASESPECIFIC,
      END_DATE_COLUMN VARCHAR(130) CHARACTER SET LATIN NOT CASESPECIFIC,
      ACTIVE INTEGER NOT NULL)
UNIQUE PRIMARY INDEX ( TABLE_NAME );

CREATE MULTISET TABLE EXEC_LOGS ,FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      RUN_ID BIGINT NOT NULL,
      LOAD_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,
      SOURCE_NAME VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL DEFAULT USER ,
      TABLE_NAME VARCHAR(130) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
      SQL_SCRIPT_SEQ INTEGER NOT NULL,
      SQL_SCRIPT CLOB(2097088000) CHARACTER SET LATIN NOT NULL,
      START_TIMESTAMP TIMESTAMP(6) NOT NULL,
      END_TIMESTAMP TIMESTAMP(6) NOT NULL,
      ERROR_CODE INTEGER,
      ERROR_MSG VARCHAR(1000) CHARACTER SET LATIN NOT CASESPECIFIC,
      ROWS_COUNT FLOAT,
      CREATED_BY VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL DEFAULT USER )
PRIMARY INDEX ( RUN_ID );

CREATE MULTISET TABLE "HISTORY" ,FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      PROCESS_NAME VARCHAR(130) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
      HISTORY_COLUMN VARCHAR(130) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL)
UNIQUE PRIMARY INDEX ( PROCESS_NAME ,HISTORY_COLUMN );

CREATE MULTISET TABLE PROCESS ,FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      PROCESS_NAME VARCHAR(130) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
      PROCESS_TYPE VARCHAR(10) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
      SOURCE_NAME VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
      SRC_DB VARCHAR(130) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
      SRC_TABLE VARCHAR(130) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
      TGT_DB VARCHAR(130) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
      TGT_TABLE VARCHAR(130) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
      APPLY_TYPE VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
      MAIN_TABLE_NAME VARCHAR(130) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
      KEY_SET_ID INTEGER,
      CODE_SET_ID INTEGER,
      DOMAIN_ID INTEGER,
      ACTIVE INTEGER NOT NULL)
UNIQUE PRIMARY INDEX ( PROCESS_NAME );

CREATE MULTISET TABLE SOURCE_SYSTEMS ,FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      SOURCE_NAME VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
      LOADING_MODE VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
      REJECTION_TABLE_NAME VARCHAR(150) CHARACTER SET LATIN NOT CASESPECIFIC,
      BUSINESS_RULES_TABLE_NAME VARCHAR(150) CHARACTER SET LATIN NOT CASESPECIFIC,
      STG_ACTIVE INTEGER NOT NULL,
      BASE_ACTIVE INTEGER NOT NULL,
      IS_SCHEDULED INTEGER NOT NULL,
      SOURCE_DB VARCHAR(150) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
      DATA_SRC_CD INTEGER,
      ACTIVE INTEGER NOT NULL)
UNIQUE PRIMARY INDEX ( SOURCE_NAME );

CREATE MULTISET TABLE STG_TABLES ,FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      TABLE_NAME VARCHAR(130) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
      SOURCE_NAME VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
      IS_TARANSACTIOANL INTEGER NOT NULL,
      ACTIVE INTEGER NOT NULL)
UNIQUE PRIMARY INDEX ( TABLE_NAME );

CREATE MULTISET TABLE TRANSFORM_KEYCOL ,FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      DB_NAME VARCHAR(130) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
      TABLE_NAME VARCHAR(130) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
      KEY_COLUMN VARCHAR(130) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL)
UNIQUE PRIMARY INDEX ( DB_NAME ,TABLE_NAME ,KEY_COLUMN );
