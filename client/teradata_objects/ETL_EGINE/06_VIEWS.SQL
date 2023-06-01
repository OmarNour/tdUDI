REPLACE VIEW /*VER.01*/ GDEV1_ETL.V_SOURCE_SYSTEMS AS LOCK ROW FOR ACCESS
select DS.*, l.lyr_db,l.src_db,l.layer_level
from GDEV1_ETL.SOURCE_SYSTEMS DS,GDEV1_ETL.LAYERS l
where l.layer_name = DS.SOURCE_LAYER;
--------------------------------------------------------------------------------------------------------
REPLACE VIEW /*VER.01*/ GDEV1_ETL.V_SOURCE_SYSTEM_TABLES AS LOCK ROW FOR ACCESS
select  t.*
		,stg_l.src_db stg_src_db		
		,stg_l.lyr_db stg_db  
		,stg_l.layer_level stg_layer_level
		,stg_l.layer_name stg_layer_name
		
		,srci_l.src_db srci_src_db
		,srci_l.lyr_db srci_db  
		,srci_l.layer_level srci_layer_level
		,srci_l.layer_name srci_layer_name
		
from GDEV1_ETL.SOURCE_SYSTEM_TABLES t,GDEV1_ETL.LAYERS stg_l ,GDEV1_ETL.LAYERS srci_l
where stg_l.layer_name = 'STG'
and srci_l.layer_name = 'SRCI';
--------------------------------------------------------------------------------------------------------
REPLACE VIEW /*VER.01*/ GDEV1_ETL.V_EDW_TABLES AS LOCK ROW FOR ACCESS
select t.*, l.lyr_db,l.src_db,l.layer_level
from GDEV1_ETL.EDW_TABLES t,GDEV1_ETL.LAYERS l
where l.layer_name = T.layer_name;
--------------------------------------------------------------------------------------------------------
REPLACE VIEW /*VER.01*/ GDEV1_ETL.V_TRANSFORM_KEYCOL AS LOCK ROW FOR ACCESS
select KC.*, l.lyr_db
from GDEV1_ETL.TRANSFORM_KEYCOL KC,GDEV1_ETL.LAYERS l
where l.layer_name = KC.LAYER_NAME;
--------------------------------------------------------------------------------------------------------
REPLACE VIEW /*VER.01*/ GDEV1_ETL.V_PROCESS AS LOCK ROW FOR ACCESS
select 
	P.*
	,tgt_l.src_db src_db 
	,tgt_l.lyr_db tgt_db
from GDEV1_ETL.PROCESS P ,GDEV1_ETL.LAYERS tgt_l
where tgt_l.layer_name = p.tgt_layer;
--------------------------------------------------------------------------------------------------------
REPLACE VIEW /*VER.01*/ GDEV1_ETL.V_EDW_SCRIPTS_COMPOSER AS LOCK ROW FOR ACCESS 
WITH 
CTE_KEY_COLS as
(
	SELECT x.lyr_db db_name, X.TABLE_NAME
		, TRIM( TRAILING  ',' FROM (XMLAGG(trim(KEY_COLUMN)|| ',' ORDER BY KEY_COLUMN) (VARCHAR(10000)))) AS KEY_COLUMNS
		, OREPLACE(TRIM( TRAILING  ',' FROM (XMLAGG('X.'||KEY_COLUMN||' = Y.'||KEY_COLUMN || ',' ORDER BY KEY_COLUMN) (VARCHAR(10000)))), ', ', ' AND ' ) AS KEY_COLUMNS_JOIN
		, TRIM( TRAILING  ',' FROM (XMLAGG(('COALESCE(X.'||KEY_COLUMN||', Y.'||KEY_COLUMN||' ) '||KEY_COLUMN)|| ',' ORDER BY KEY_COLUMN) (VARCHAR(10000)))) AS COALESCE_KEY_COLUMN
	FROM GDEV1_ETL.V_TRANSFORM_KEYCOL X
	WHERE NOT EXISTS (
						SELECT 1 
						FROM GDEV1_ETL.V_EDW_TABLES Y 
						WHERE X.LAYER_NAME= Y.LAYER_NAME
						AND Y.TABLE_NAME=X.TABLE_NAME
						AND Y.START_DATE_COLUMN = X.KEY_COLUMN
						AND Y.IS_HISTORY = 1
					)
	GROUP BY x.lyr_db, X.TABLE_NAME
)
, CTE_HISTORY_COLS AS
(
	SELECT PROCESS_NAME
	, TRIM( TRAILING  ',' FROM (XMLAGG(trim(HISTORY_COLUMN)|| ',' ORDER BY HISTORY_COLUMN) (VARCHAR(10000)))) HISTORY_COLUMNS
	, TRIM( TRAILING  ',' FROM (XMLAGG(trim(HISTORY_COLUMN||' = X.'||HISTORY_COLUMN)|| ',' ORDER BY HISTORY_COLUMN) (VARCHAR(10000)))) SET_HISTORY_COLUMNS
	, TRIM( TRAILING  ',' FROM (XMLAGG(trim('X.'||HISTORY_COLUMN)|| ',' ORDER BY HISTORY_COLUMN) (VARCHAR(10000)))) X_HISTORY_COLUMNS
	, TRIM( TRAILING  ',' FROM (XMLAGG(trim('COALESCE(X.'||HISTORY_COLUMN||','''')') ||HISTORY_COLUMN||'_X,' ORDER BY HISTORY_COLUMN) (VARCHAR(10000)))) COALESCE_HIST_COLX
	, TRIM( TRAILING  ',' FROM (XMLAGG(trim('COALESCE(Y.'||HISTORY_COLUMN||','''')') ||HISTORY_COLUMN||'_Y,' ORDER BY HISTORY_COLUMN) (VARCHAR(10000)))) COALESCE_HIST_COLY
	, TRIM( TRAILING  '|' FROM (XMLAGG(HISTORY_COLUMN|| '_X||' ORDER BY HISTORY_COLUMN) (VARCHAR(10000)))) 	|| ' HC_X'					CONCATE_HIST_COL_X
	, TRIM( TRAILING  '|' FROM (XMLAGG(HISTORY_COLUMN|| '_Y||' ORDER BY HISTORY_COLUMN) (VARCHAR(10000))))	|| ' HC_Y'					CONCATE_HIST_COL_Y
	
	FROM GDEV1_ETL.HISTORY
	GROUP BY PROCESS_NAME
)
, CTE_PROCESS AS
(
	SELECT 
	P.PROCESS_NAME,
    P.SOURCE_NAME,
    P.SRC_DB,
    P.SRC_TABLE,
    P.TGT_DB,
    P.TGT_TABLE,
    P.tgt_layer,
    CASE WHEN TBL.IS_HISTORY = 1 THEN 'HISTORY' ELSE P.APPLY_TYPE END APPLY_TYPE,
    P.MAIN_TABLE_NAME,
    P.KEY_SET_ID,
    P.CODE_SET_ID,
    P.DOMAIN_ID,
    P.ACTIVE
    FROM GDEV1_ETL.V_PROCESS P
    	JOIN GDEV1_ETL.V_EDW_TABLES TBL
    	ON P.TGT_TABLE = TBL.TABLE_NAME
		AND P.tgt_layer = TBL.layer_name
)
, CTE_NORMAL_COMMON_COLS as
(
	SELECT
	PROCESS_NAME
	, TRIM( TRAILING  ',' FROM (XMLAGG(trim(columnname)|| ',' ORDER BY columnname) (VARCHAR(10000)))) NORMAL_COMMON_COLUMNS
	, TRIM( TRAILING  ',' FROM (XMLAGG('X.'||columnname || ',' ORDER BY columnname) (VARCHAR(10000)))) X_NORMAL_COMMON_COLUMNS
	, TRIM( TRAILING  ',' FROM (XMLAGG(trim(columnname||' = X.'||columnname)|| ',' ORDER BY columnname) (VARCHAR(10000)))) SET_NORMAL_COMMON_COLUMNS
	FROM 
	(
		select P.PROCESS_NAME, V.columnname 
		from dbc.columnsv v
		join CTE_PROCESS P 
		on P.SRC_DB=v.databasename and P.SRC_TABLE=v.tablename
		and V.columnname not in ('BATCH_ID', 'MODIFICATION_TYPE', 'REF_KEY')
		
		INTERSECT
		
		select P.PROCESS_NAME, columnname 
		from dbc.columnsv v
		join CTE_PROCESS P 
		on P.TGT_DB=v.databasename and P.TGT_TABLE=v.tablename
		AND NOT EXISTS (
							SELECT 1 
							FROM GDEV1_ETL.V_EDW_TABLES Y 
							WHERE v.databasename=Y.lyr_db
							AND v.tablename=y.TABLE_NAME
							AND (v.columnname = Y.START_DATE_COLUMN or v.columnname = Y.END_DATE_COLUMN)
							AND Y.IS_HISTORY = 1
							AND P.APPLY_TYPE = 'HISTORY'
						)
		AND NOT EXISTS (
							SELECT 1 
							FROM GDEV1_ETL.V_TRANSFORM_KEYCOL Y 
							WHERE v.databasename=Y.lyr_db
							AND v.tablename=y.TABLE_NAME
							AND v.columnname = Y.KEY_COLUMN
						)
		AND NOT EXISTS (
							SELECT 1 
							FROM GDEV1_ETL.HISTORY Y 
							WHERE P.PROCESS_NAME=y.PROCESS_NAME
							AND v.columnname = Y.HISTORY_COLUMN
							AND P.APPLY_TYPE = 'HISTORY'
						)
	) X
	GROUP BY PROCESS_NAME
)
SELECT
	P.PROCESS_NAME,
    P.SOURCE_NAME,
    P.SRC_DB,
    P.SRC_TABLE,
    P.TGT_DB,
    P.TGT_TABLE,
   	KC.KEY_COLUMNS,
   	KC.KEY_COLUMNS_JOIN,
   	KC.COALESCE_KEY_COLUMN,
   	CTBL.START_DATE_COLUMN,
   	CTBL.END_DATE_COLUMN,
    HC.HISTORY_COLUMNS,
    HC.X_HISTORY_COLUMNS,
    HC.COALESCE_HIST_COLX,
    HC.COALESCE_HIST_COLY,
    HC.CONCATE_HIST_COL_X,
    HC.CONCATE_HIST_COL_Y,
    HC.SET_HISTORY_COLUMNS,
    P.APPLY_TYPE,
    P.KEY_SET_ID,
    P.CODE_SET_ID,
    P.DOMAIN_ID,
    P.ACTIVE,
    NCC.NORMAL_COMMON_COLUMNS,
    NCC.X_NORMAL_COMMON_COLUMNS,
    NCC.SET_NORMAL_COMMON_COLUMNS
FROM CTE_PROCESS P
	
	LEFT JOIN CTE_KEY_COLS KC
	ON P.TGT_DB=KC.DB_NAME
	AND P.TGT_TABLE=KC.TABLE_NAME
	
	LEFT JOIN GDEV1_ETL.V_EDW_TABLES CTBL
	ON CTBL.TABLE_NAME=P.TGT_TABLE
	AND CTBL.layer_name = P.tgt_layer
	AND CTBL.IS_HISTORY = 1
	AND P.APPLY_TYPE = 'HISTORY'
	
	LEFT JOIN CTE_HISTORY_COLS HC
	ON HC.PROCESS_NAME=P.PROCESS_NAME
	
	LEFT JOIN CTE_NORMAL_COMMON_COLS NCC
	ON NCC.PROCESS_NAME=P.PROCESS_NAME;
--------------------------------------------------------------------------------------------------------

