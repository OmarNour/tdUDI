CREATE MULTISET TABLE GDEV1T_GCFR.EXEC_LOGS
    ,FALLBACK
    ,NO BEFORE JOURNAL
    ,NO AFTER JOURNAL
    ,CHECKSUM = DEFAULT
    ,DEFAULT MERGEBLOCKRATIO
(
	  RUN_ID            BIGINT          not null
    , LOAD_ID           VARCHAR(100)    CHARACTER SET latin not CASESPECIFIC
    , SOURCE_NAME       VARCHAR(50)     CHARACTER SET latin not CASESPECIFIC not null
    , TABLE_NAME        VARCHAR(130)    CHARACTER SET latin not CASESPECIFIC not null
    , SQL_SCRIPT_SEQ    INTEGER         not null
    , SQL_SCRIPT        VARCHAR(130)    CHARACTER SET latin not CASESPECIFIC not null
    , START_TIMESTAMP   TIMESTAMP(6)    NOT NULL
    , END_TIMESTAMP     TIMESTAMP(6)    NOT NULL
    , ERROR_CODE        INTEGER
    , ERROR_MSG         VARCHAR(1000)
    , ROWS_COUNT        BIGINT
 )
UNIQUE PRIMARY INDEX ( TABLE_NAME);