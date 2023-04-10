drop table GDEV1T_GCFR.SOURCE_SYSTEMS;
drop table GDEV1T_GCFR.STG_TABLES;
drop table GDEV1T_GCFR.CORE_TABLES;
drop table GDEV1T_GCFR.TRANSFORM_KEYCOL;
drop table GDEV1T_GCFR.PROCESS;
drop table GDEV1T_GCFR.HISTORY;

CREATE MULTISET TABLE GDEV1T_GCFR.SOURCE_SYSTEMS
    ,FALLBACK
    ,NO BEFORE JOURNAL
    ,NO AFTER JOURNAL
    ,CHECKSUM = DEFAULT
    ,DEFAULT MERGEBLOCKRATIO
(
      SOURCE_NAME                   VARCHAR(50)     CHARACTER SET latin not CASESPECIFIC not null
    , SOURCE_MODE                   VARCHAR(20)     CHARACTER SET latin not CASESPECIFIC not null -- online or offline
    , REJECTION_TABLE_NAME          VARCHAR(150)    CHARACTER SET latin not CASESPECIFIC
    , BUSINESS_RULES_TABLE_NAME     VARCHAR(150)    CHARACTER SET latin not CASESPECIFIC
 	, STG_ACTIVE                    INTEGER         not null
    , BASE_ACTIVE                   INTEGER         not null
    , IS_SCHEDULED                  INTEGER         not null
    , SOURCE_DB                     VARCHAR(150)    not null
    , DATA_SRC_CD                   INTEGER
 	, ACTIVE                        INTEGER         not null
 )
UNIQUE PRIMARY INDEX ( SOURCE_NAME );

CREATE MULTISET TABLE GDEV1T_GCFR.STG_TABLES
    ,FALLBACK
    ,NO BEFORE JOURNAL
    ,NO AFTER JOURNAL
    ,CHECKSUM = DEFAULT
    ,DEFAULT MERGEBLOCKRATIO
(
	  TABLE_NAME            VARCHAR(130) CHARACTER SET latin not CASESPECIFIC not null
    , SOURCE_NAME           VARCHAR(50) CHARACTER SET latin not CASESPECIFIC not null
 	, IS_TARANSACTIOANL     INTEGER   not null
 	, ACTIVE                INTEGER   not null
 )
UNIQUE PRIMARY INDEX (TABLE_NAME);

CREATE MULTISET TABLE GDEV1T_GCFR.CORE_TABLES
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

CREATE MULTISET TABLE GDEV1T_GCFR.TRANSFORM_KEYCOL
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

CREATE MULTISET TABLE GDEV1T_GCFR.PROCESS
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
    , TGT_DB        VARCHAR(130)    CHARACTER SET latin not CASESPECIFIC not null
    , TGT_TABLE     VARCHAR(130)    CHARACTER SET latin not CASESPECIFIC not null
    , APPLY_TYPE    VARCHAR(20)     CHARACTER SET latin not CASESPECIFIC not null
    , KEY_SET_ID    INTEGER
    , CODE_SET_ID   INTEGER
    , DOMAIN_ID     INTEGER
    , ACTIVE        INTEGER NOT NULL
 )
UNIQUE PRIMARY INDEX ( PROCESS_NAME);

CREATE MULTISET TABLE GDEV1T_GCFR.HISTORY
    ,FALLBACK
    ,NO BEFORE JOURNAL
    ,NO AFTER JOURNAL
    ,CHECKSUM = DEFAULT
    ,DEFAULT MERGEBLOCKRATIO
(
	  PROCESS_NAME  VARCHAR(130)    CHARACTER SET latin not CASESPECIFIC not null
    , HISTORY_COLUMN   VARCHAR(130)    CHARACTER SET latin not CASESPECIFIC not null
 )
UNIQUE PRIMARY INDEX ( PROCESS_NAME, HISTORY_COLUMN);