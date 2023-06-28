INSERT	INTO GDEV1_ETL.SOURCE_SYSTEMS
(SOURCE_NAME, LOADING_MODE, REJECTION_TABLE_NAME, BUSINESS_RULES_TABLE_NAME,
		STG_ACTIVE, BASE_ACTIVE, IS_SCHEDULED, SOURCE_LAYER, DATA_SRC_CD,
		ACTIVE)

select distinct
TERADATA_SOURCE_NAME SOURCE_NAME
,LOADING_TYPE LOADING_MODE
,REJECTION_TABLE_NAME REJECTION_TABLE_NAME
, business_rules_table_name BUSINESS_RULES_TABLE_NAME
,STG_LAYER STG_ACTIVE
,BASE_LAYER BASE_ACTIVE
,IS_SCHEDULED IS_SCHEDULED
,'SRC' SOURCE_LAYER
, DATA_SRC_CD DATA_SRC_CD
,ACTIVE
from gdevp1t_gcfr.source_name_lkp;

------------------------------------------------------------------------------------------
INSERT	INTO GDEV1_ETL.SOURCE_SYSTEM_TABLES
(TABLE_NAME, SOURCE_NAME, IS_TARANSACTIOANL, ACTIVE)

select distinct TABLE_NAME, SOURCE_NAME, 0, ACTIVE  from gdevp1t_gcfr.source_tables_lkp;
------------------------------------------------------------------------------------------
INSERT	INTO GDEV1_ETL.EDW_TABLES
(LAYER_NAME, TABLE_NAME, IS_LOOKUP, IS_HISTORY, START_DATE_COLUMN, END_DATE_COLUMN, ACTIVE)
		
select *
from
(
	select 
	case when PROCESS_TYPE = 'txf' then 'CORE' when PROCESS_TYPE = 'BKEY' then 'BKEY' end LAYER_NAME
	,base_table TABLE_NAME
	,0 IS_LOOKUP
	,case when APPLY_TYPE = 'history' then 1 else 0 end IS_HISTORY
	, START_DATE_COLUMN
	,END_DATE_COLUMN
	, active
	 from gdevp1t_gcfr.etl_process p
		LEFT JOIN gdevp1t_gcfr.history h
		on p.process_name = h.process_name

) x
	where LAYER_NAME is not null and TABLE_NAME is not null
qualify ROW_NUMBER() OVER (partition by LAYER_NAME, TABLE_NAME order by active desc, IS_HISTORY desc) = 1;
------------------------------------------------------------------------------------------
INSERT	INTO GDEV1_ETL.TRANSFORM_KEYCOL
(LAYER_NAME, TABLE_NAME, KEY_COLUMN)

SELECT 
l.layer_name
,Out_Object_Name TABLE_NAME
,Key_Column
FROM GDEVP1T_GCFR.GCFR_TRANSFORM_KEYCOL, GDEV1_ETL.LAYERS L
where Out_DB_Name = l.lyr_db
and l.layer_name in ('STG', 'SRCI');
------------------------------------------------------------------------------------------
INSERT	INTO GDEV1_ETL.TRANSFORM_KEYCOL
(LAYER_NAME, TABLE_NAME, KEY_COLUMN)

SELECT 
l.layer_name
,Out_Object_Name TABLE_NAME
,Key_Column
FROM GDEVP1T_GCFR.GCFR_TRANSFORM_KEYCOL, GDEV1_ETL.LAYERS L
where Out_DB_Name = l.lyr_db
and l.layer_name = 'CORE';
------------------------------------------------------------------------------------------
INSERT	INTO GDEV1_ETL.PROCESS
(PROCESS_NAME, SOURCE_NAME, TGT_LAYER, TGT_TABLE, SRC_TABLE, APPLY_TYPE,
MAIN_TABLE_NAME, KEY_SET_ID, CODE_SET_ID, DOMAIN_ID, ACTIVE)
SELECT * FROM
(
select 
p.PROCESS_NAME
,SOURCE_NAME
,case when p.PROCESS_TYPE = 'BKEY' then 'BKEY'  when p.PROCESS_TYPE = 'TXF' THEN 'CORE' end TGT_LAYER
,BASE_TABLE TGT_TABLE
, p.PROCESS_NAME || '_IN' SRC_TABLE
, APPLY_TYPE
, COALESCE(STG_TABLE_NAME,'') MAIN_TABLE_NAME

			
, g.Key_Set_Id KEY_SET_ID
, null CODE_SET_ID
, g.Domain_Id DOMAIN_ID
, ACTIVE
from GDEVp1t_gcfr.etl_process p
	LEFT join GDEVP1V_GCFR.GCFR_PROCESS g
	on p.PROCESS_NAME=g.PROCESS_NAME
where TGT_LAYER IS NOT NULL
) X
qualify ROW_NUMBER() OVER (partition by PROCESS_NAME order by active desc) = 1;
------------------------------------------------------------------------------------------
INSERT	INTO GDEV1_ETL.HISTORY
(PROCESS_NAME, HISTORY_COLUMN)

select distinct PROCESS_NAME, HISTORY_COLUMN
from GDEVp1t_gcfr.history
where HISTORY_COLUMN is not null;
------------------------------------------------------------------------------------------
select 
'alter TABLE '||lyr_db||'.'||table_name||' add UPD_DTTM TIMESTAMP(6);'
from GDEV1_ETL.SOURCE_SYSTEM_TABLES,GDEV1_ETL.LAYERS l
where  l.layer_name = 'STG';
------------------------------------------------------------------------------------------
select 
'alter TABLE '||lyr_db||'.'||table_name||' add Ctl_Id SMALLINT NULL;'
from GDEV1_ETL.EDW_TABLES,GDEV1_ETL.LAYERS l
where  l.layer_name = 'BKEY'
and exists (select 1 from DBC.TablesV v where v.databasename = lyr_db and v.tablename = table_name);
------------------------------------------------------------------------------------------
select 
'alter TABLE '||lyr_db||'.'||table_name||' add Process_Id INTEGER NULL;'
from GDEV1_ETL.EDW_TABLES,GDEV1_ETL.LAYERS l
where  l.layer_name = 'BKEY'
and exists (select 1 from DBC.TablesV v where v.databasename = lyr_db and v.tablename = table_name);
------------------------------------------------------------------------------------------
select 
'alter TABLE '||lyr_db||'.'||table_name||' add BATCH_ID INTEGER NULL  DEFAULT 0;'
from GDEV1_ETL.EDW_TABLES,GDEV1_ETL.LAYERS l
where  l.layer_name = 'CORE'
and exists (select 1 from DBC.TablesV v where v.databasename = lyr_db and v.tablename = table_name);
------------------------------------------------------------------------------------------
select 
'alter TABLE '||lyr_db||'.'||table_name||' add UPDATE_TS TIMESTAMP(6);'
from GDEV1_ETL.EDW_TABLES,GDEV1_ETL.LAYERS l
where  l.layer_name = 'CORE'
and exists (select 1 from DBC.TablesV v where v.databasename = lyr_db and v.tablename = table_name);
------------------------------------------------------------------------------------------
-- drop tables in WRK DB, and move space from this DB to any as per needs
select 
'drop table '||databasename||'.'||tablename||';'
from DBC.tablesv where databasename = 'GDEVP1T_WRK';
