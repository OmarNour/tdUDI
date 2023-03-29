from collections import namedtuple

smx_path = "/Users/omarnour/Downloads/Production_Citizen_SMX.xlsx"
# smx_path = "/Users/omarnour/Downloads/[ACA] SMX_Economic_Units_03-01-2023.xlsx"
# smx_path = "/Users/omarnour/Downloads/Wasl Schema v1.7.xlsx"
# smx_path = "/Users/omarnour/Downloads/SALES_SMX.xlsx"

pickle_path = "pickled_objs"
scripts_path = "/Users/omarnour/Downloads/smx_scripts"
DB_NAME = 'teradata'
HOST = 'localhost'
USER = 'dbc'
PASSWORD = 'dbc'

cls_keys = {
    'server': 'server_name'
    , 'Ip': ('server_id', 'ip')
    , 'DataBaseEngine': ('server_id', 'name')
    , 'Credential': ('db_engine_id', 'user_name')
    , 'datasource': 'source_name'
    , 'schema': ('db_id', 'schema_name')
    , 'table': ('schema_id', 'table_name')
    , 'DataSetType': 'name'
    , 'DataSet': ('set_type_id', 'set_code')
    , 'Domain': ('data_set_id', 'domain_code')
    , 'DomainValue': ('domain_id', 'source_key')
    , 'Column': ('table_id', 'column_name')
    , 'DataType': ('db_id', 'dt_name')
    , 'LayerType': 'type_name'
    , 'Layer': 'layer_name'
    , 'LayerTable': ('layer_id', 'table_id')
    , 'Pipeline': 'lyr_view_id'
    , 'ColumnMapping': ('pipeline_id', 'col_seq', 'tgt_col_id')
    , 'Filter': ('pipeline_id', 'filter_seq')
    , 'GroupBy': ('pipeline_id', 'col_id')
    , 'JoinType': 'code'
    , 'JoinWith': ('pipeline_id', 'with_lyr_table_id', 'with_alias')
    , 'joinOn': 'join_with_id'
}

SHEETS = ['stg_tables', 'system', 'data_type', 'bkey', 'bmap'
    , 'bmap_values', 'core_tables', 'column_mapping', 'table_mapping'
    , 'supplements']
UNIFIED_SOURCE_SYSTEMS = ['UNIFIED_GOV',
                          'UNIFIED_CITY',
                          'UNIFIED_POLICE_STATION',
                          'UNIFIED_DISTRICT',
                          'UNIFIED_COUNTRY']
DS_BKEY = 'BKEY'
DS_BMAP = 'BMAP'
LayerDtl = namedtuple("LayerDetail", "type level v_db t_db")
JoinTypes = namedtuple("JoinTypes", "code name")
JOIN_TYPES = [JoinTypes(code='ij', name='inner join'), JoinTypes(code='lj', name='left join'),
              JoinTypes(code='rj', name='right join'), JoinTypes(code='fj', name='full outer join')]
LAYER_TYPES = ['META', 'SRC', 'STG', 'SK', 'SRCI', 'CORE']
PREFIX = 'GDEV1'
LAYERS = {
    'META': LayerDtl(LAYER_TYPES[0], 0, f'{PREFIX}V_GCRF', f'{PREFIX}T_GCRF')
    ,'SRC': LayerDtl(LAYER_TYPES[1], 0, f'{PREFIX}V_STG_ONLINE', 'STG_ONLINE')
    , 'STG': LayerDtl(LAYER_TYPES[2], 1, f'{PREFIX}V_STG', f'{PREFIX}T_STG')
    , 'TXF_BKEY': LayerDtl(LAYER_TYPES[2], 2, f'{PREFIX}V_INP', '')
    , 'BKEY': LayerDtl(LAYER_TYPES[3], 3, f'{PREFIX}V_UTLFW', f'{PREFIX}T_UTLFW')
    , 'BMAP': LayerDtl(LAYER_TYPES[3], 3, f'{PREFIX}V_UTLFW', f'{PREFIX}T_UTLFW')
    , 'SRCI': LayerDtl(LAYER_TYPES[4], 4, f'{PREFIX}V_SRCI', f'{PREFIX}T_SRCI')
    , 'TXF_CORE': LayerDtl(LAYER_TYPES[4], 5, f'{PREFIX}V_INP', '')
    , 'CORE': LayerDtl(LAYER_TYPES[5], 6, f'{PREFIX}V_BASE', f'{PREFIX}T_BASE')
}

NUMBERS = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
ALPHABETS = ['a', 'b', 'c', 'd', 'e', 'f', 'g'
    , 'h', 'i', 'j', 'k', 'l', 'm', 'n'
    , 'o', 'p', 'q', 'r', 's', 't', 'u'
    , 'v', 'w', 'x', 'y', 'z']
SPECIAL_CHARACTERS = [
    '!', '"', '#', '$', '%', '&', "'", '(', ')', '*', '+', ','
    , '-', '.', '/', ':', ';', '<', '=', '>', '?', '@', '['
    , '\\', ']', '^', '_', '`', '{', '|', '}', '~'
]
SRC_SYSTEMS_FOLDER_NAME = "SOURCES"
CORE_MODEL_FOLDER_NAME = "CORE_MODEL"
CAST_DTYPE_TEMPLATE = """({dtype_name} {precise})"""
COL_MAPPING_TEMPLATE = """{comma}{col_name} {cast_dtype} {alias}"""
FROM_TEMPLATE = """from {schema_name}.{table_name} {alias}"""
WHERE_TEMPLATE = """where {conditions}"""
GROUP_BY_TEMPLATE = """group by {columns}"""
PI_TEMPLATE = """PRIMARY INDEX ( {pi_cols} )"""
COL_DTYPE_TEMPLATE = """\t{comma}{col_name}  {data_type}{precision} {latin_unicode} {case_sensitive} {not_null}\n """
DDL_TABLE_TEMPLATE = """ 
CREATE {set_multiset} TABLE {schema_name}.{table_name}
    ,FALLBACK
    ,NO BEFORE JOURNAL
    ,NO AFTER JOURNAL
    ,CHECKSUM = DEFAULT
    ,DEFAULT MERGEBLOCKRATIO
(\n{col_dtype})
{pi_index}
{si_index}
 """
DDL_VIEW_TEMPLATE = """CREATE VIEW /*VER.1*/  {schema_name}.{view_name} AS LOCK ROW FOR ACCESS {query_txt}"""
QUERY_TEMPLATE = """ {with_clause}\nselect {distinct}\n{col_mapping}\n{from_clause} {join_clause}\n{where_clause}\n{group_by_clause}\n{having_clause}"""
JOIN_CLAUSE_TEMPLATE = "\n\t{join_type} {with_table} {with_alias}\n\ton {on_clause}"
SRCI_V_BKEY_TEMPLATE_QUERY = """(select EDW_KEY\n from {bkey_db}.{bkey_table_name}\n where SOURCE_KEY = {src_key} {cast}\n and DOMAIN_ID={domain_id})"""
SRCI_V_BMAP_TEMPLATE_QUERY = """(select EDW_Code\n from {bmap_db}.{bmap_table_name}\n where SOURCE_CODE = {source_code} {cast}\n and CODE_SET_ID = {code_set_id}\n and DOMAIN_ID={domain_id})"""
BK_VIEW_NAME_TEMPLATE = "BKEY_L{src_lvl}_{src_table_name}_{column_name}_L{tgt_lvl}_{domain_id}"
CORE_VIEW_NAME_TEMPLATE = "TXF_CORE_{mapping_name}"
DATABASE_TEMPLATE = """
CREATE DATABASE {db_name}
AS PERMANENT = 60e6, -- 60MB
    SPOOL = 120e6; -- 120MB
"""

##################### Metadata ####################
INSERT_INTO_SOURCE_NAME_LKP = """
INSERT INTO {meta_db}.SOURCE_NAME_LKP (
    SOURCE_NAME,
    rejection_table_name,
    business_rules_table_name,
    LOADING_TYPE,
    ACTIVE,
    PRIORITY,
    STG_LAYER,
    BASE_LAYER,
    PL_LAYER,
    IS_SCHEDULED,
    IS_BATCH_FULL_DUMP,
    SOURCE_DB,
    DATA_SRC_CD
) VALUES (
    '{SOURCE_NAME}',
    '{rejection_table_name}',
    '{business_rules_table_name}',
    '{LOADING_TYPE}',
    1,
    100,
    1,
    1,
    0,
    1,
    1,
    '{SOURCE_DB}',
    '{DATA_SRC_CD}'
    );
"""

INSERT_INTO_SOURCE_TABLES_LKP = """
INSERT INTO {meta_db}.SOURCE_TABLES_LKP (SOURCE_NAME, TABLE_NAME, active, TRANSACTION_DATA)
VALUES ('{SOURCE_NAME}', '{TABLE_NAME}', 1, '{TRANSACTION_DATA}');
"""
INSERT_INTO_GCFR_TRANSFORM_KEYCOL="""
INSERT INTO {meta_db}.GCFR_Transform_KeyCol (Out_DB_Name, Out_Object_Name, Key_Column)
VALUES ('{OUT_DB_NAME}', '{OUT_OBJECT_NAME}', '{KEY_COLUMN}');
"""
INSERT_INTO_HISTORY = """
INSERT INTO {meta_db}.HISTORY 
    (TRF_TABLE_NAME, PROCESS_NAME, TABLE_NAME, START_DATE_COLUMN, END_DATE_COLUMN, HISTORY_COLUMN, HISTORY_KEY) 
VALUES 
    ('{TABLE_NAME}', '{PROCESS_NAME}', '{TABLE_NAME}', '{START_DATE_COLUMN}', '{END_DATE_COLUMN}', '{HISTORY_COLUMN}', '{HISTORY_KEY}');

"""
INSERT_INTO_ETL_PROCESS = """
INSERT INTO {meta_db}.ETL_PROCESS 
    (SOURCE_NAME, PROCESS_TYPE, PROCESS_NAME, BASE_TABLE, APPLY_TYPE, active
    , INPUT_VIEW_DB, TARGET_TABLE_DB, TARGET_VIEW_DB, SRCI_TABLE_DB, SRCI_TABLE_NAME, Key_Set_Id, Domain_Id, Code_Set_Id) 
VALUES ('{SOURCE_NAME}', '{PROCESS_TYPE}', '{PROCESS_NAME}', '{BASE_TABLE}', '{APPLY_TYPE}', 1
    , '{INPUT_VIEW_DB}', '{TARGET_TABLE_DB}', '{TARGET_VIEW_DB}', '{SRCI_TABLE_DB}', '{SRCI_TABLE_NAME}');
"""

