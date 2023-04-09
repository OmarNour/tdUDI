drop table GDEV1T_GCRF.SOURCE_NAME_LKP;
drop table GDEV1T_GCRF.SOURCE_TABLES_LKP;
drop table GDEV1T_GCRF.CORE_TABLES_LKP;
drop table GDEV1T_GCRF.GCFR_TRANSFORM_KEYCOL;
drop table GDEV1T_GCRF.ETL_PROCESS;
drop table GDEV1T_GCRF.HISTORY;

CREATE MULTISET TABLE GDEV1T_GCRF.SOURCE_NAME_LKP
    ,FALLBACK
    ,NO BEFORE JOURNAL
    ,NO AFTER JOURNAL
    ,CHECKSUM = DEFAULT
    ,DEFAULT MERGEBLOCKRATIO
(
      SOURCE_NAME                   VARCHAR(50)     CHARACTER SET latin not CASESPECIFIC not null
    , LOADING_TYPE                  VARCHAR(20)     CHARACTER SET latin not CASESPECIFIC not null -- online or offline
    , REJECTION_TABLE_NAME          VARCHAR(150)    CHARACTER SET latin not CASESPECIFIC null
    , BUSINESS_RULES_TABLE_NAME     VARCHAR(150)    CHARACTER SET latin not CASESPECIFIC null
 	, STG_LAYER                     INTEGER   not null
    , BASE_LAYER                    INTEGER   not null
    , IS_SCHEDULED                  INTEGER   not null
    , SOURCE_DB                     INTEGER   not null
    , DATA_SRC_CD                   INTEGER   not null
 	, ACTIVE                        INTEGER   not null
 )
UNIQUE PRIMARY INDEX ( SOURCE_NAME );

CREATE MULTISET TABLE GDEV1T_GCRF.SOURCE_TABLES_LKP
    ,FALLBACK
    ,NO BEFORE JOURNAL
    ,NO AFTER JOURNAL
    ,CHECKSUM = DEFAULT
    ,DEFAULT MERGEBLOCKRATIO
(
	  TABLE_NAME    VARCHAR(130) CHARACTER SET latin not CASESPECIFIC not null
    , SOURCE_NAME   VARCHAR(50) CHARACTER SET latin not CASESPECIFIC not null
 	, TRANSACTION_DATA  INTEGER   not null
 	, ACTIVE  INTEGER   not null
 )
UNIQUE PRIMARY INDEX (TABLE_NAME);

CREATE MULTISET TABLE GDEV1T_GCRF.CORE_TABLES_LKP
    ,FALLBACK
    ,NO BEFORE JOURNAL
    ,NO AFTER JOURNAL
    ,CHECKSUM = DEFAULT
    ,DEFAULT MERGEBLOCKRATIO
(
	  TABLE_NAME        VARCHAR(130) CHARACTER SET latin not CASESPECIFIC not null
 	, IS_LOOKUP         INTEGER   not null
    , IS_HISTORY        INTEGER   not null
    , START_DATE_COLUMN VARCHAR(130) CHARACTER SET latin not CASESPECIFIC
    , END_DATE_COLUMN   VARCHAR(130) CHARACTER SET latin not CASESPECIFIC
 	, ACTIVE            INTEGER   not null
 )
UNIQUE PRIMARY INDEX ( TABLE_NAME );

CREATE MULTISET TABLE GDEV1T_GCRF.GCFR_TRANSFORM_KEYCOL
    ,FALLBACK
    ,NO BEFORE JOURNAL
    ,NO AFTER JOURNAL
    ,CHECKSUM = DEFAULT
    ,DEFAULT MERGEBLOCKRATIO
(
	  DB_NAME       VARCHAR(130) CHARACTER SET latin not CASESPECIFIC not null
    , TABLE_NAME    VARCHAR(130) CHARACTER SET latin not CASESPECIFIC not null
    , KEY_COLUMN    VARCHAR(130) CHARACTER SET latin not CASESPECIFIC not null
 )
UNIQUE PRIMARY INDEX ( DB_NAME, TABLE_NAME, KEY_COLUMN);

CREATE MULTISET TABLE GDEV1T_GCRF.ETL_PROCESS
    ,FALLBACK
    ,NO BEFORE JOURNAL
    ,NO AFTER JOURNAL
    ,CHECKSUM = DEFAULT
    ,DEFAULT MERGEBLOCKRATIO
(
	  PROCESS_NAME  VARCHAR(130)    CHARACTER SET latin not CASESPECIFIC not null
    , PROCESS_TYPE  VARCHAR(10)     CHARACTER SET latin not CASESPECIFIC not null
    , SOURCE_NAME   VARCHAR(50)     CHARACTER SET latin not CASESPECIFIC not null
    , SRC_DB        VARCHAR(130)    CHARACTER SET latin not CASESPECIFIC not null
    , SRC_TABLE     VARCHAR(130)    CHARACTER SET latin not CASESPECIFIC not null
    , TARGET_DB     VARCHAR(130)    CHARACTER SET latin not CASESPECIFIC not null
    , TARGET_TABLE  VARCHAR(130)    CHARACTER SET latin not CASESPECIFIC not null
    , APPLY_TYPE    VARCHAR(10)     CHARACTER SET latin not CASESPECIFIC not null
    , KEY_SET_ID    INTEGER NOT
    , CODE_SET_ID   INTEGER NOT
    , DOMAIN_ID     INTEGER NOT
    , ACTIVE        INTEGER NOT NULL
 )
UNIQUE PRIMARY INDEX ( PROCESS_NAME);

CREATE MULTISET TABLE GDEV1T_GCRF.HISTORY
    ,FALLBACK
    ,NO BEFORE JOURNAL
    ,NO AFTER JOURNAL
    ,CHECKSUM = DEFAULT
    ,DEFAULT MERGEBLOCKRATIO
(
	  PROCESS_NAME  VARCHAR(130)    CHARACTER SET latin not CASESPECIFIC not null
    , HISTORY_COL   VARCHAR(130)    CHARACTER SET latin not CASESPECIFIC not null
 )
UNIQUE PRIMARY INDEX ( PROCESS_NAME, HISTORY_COL);