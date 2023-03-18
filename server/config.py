from collections import namedtuple

smx_path = "/Users/omarnour/Downloads/Production_Citizen_SMX.xlsx"
# smx_path = "/Users/omarnour/Downloads/SALES_SMX.xlsx"
pickle_path = "pickled_objs"
scripts_path = "/Users/omarnour/Downloads/smx_scripts"
DB_NAME = 'teradata'
cls_keys = {
    'server': 'server_name'
    , 'DataBaseEngine': 'name'
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

DS_BKEY = 'BKEY'
DS_BMAP = 'BMAP'
LayerDtl = namedtuple("LayerDetail", "type level v_db t_db")
JoinTypes = namedtuple("JoinTypes", "code name")
JOIN_TYPES = [JoinTypes(code='ij', name='inner join'), JoinTypes(code='lj', name='left join'),
              JoinTypes(code='rj', name='right join'), JoinTypes(code='fj', name='full outer join')]
LAYER_TYPES = ['SRC', 'STG', 'SK', 'SRCI', 'CORE']
PREFIX = 'GDEV1'
LAYERS = {
    'SRC': LayerDtl(LAYER_TYPES[0], 0, f'{PREFIX}V_STG_ONLINE', 'STG_ONLINE')
    , 'STG': LayerDtl(LAYER_TYPES[1], 1, f'{PREFIX}V_STG', f'{PREFIX}T_STG')
    , 'TXF_BKEY': LayerDtl(LAYER_TYPES[1], 2, f'{PREFIX}V_INP', '')
    , 'BKEY': LayerDtl(LAYER_TYPES[2], 3, f'{PREFIX}V_UTLFW', f'{PREFIX}T_UTLFW')
    , 'BMAP': LayerDtl(LAYER_TYPES[2], 3, f'{PREFIX}V_UTLFW', f'{PREFIX}T_UTLFW')
    , 'SRCI': LayerDtl(LAYER_TYPES[3], 4, f'{PREFIX}V_SRCI', f'{PREFIX}T_SRCI')
    , 'TXF_CORE': LayerDtl(LAYER_TYPES[3], 5, f'{PREFIX}V_INP', '')
    , 'CORE': LayerDtl(LAYER_TYPES[4], 6, f'{PREFIX}V_BASE', f'{PREFIX}T_BASE')
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
SRCI_V_BKEY_TEMPLATE_QUERY = """(select EDW_KEY\n from {bkey_db}.{bkey_table_name}\n where SOURCE_KEY = {src_key}\n and DOMAIN_ID={domain_id})"""
SRCI_V_BMAP_TEMPLATE_QUERY = """(select EDW_Code\n from {bmap_db}.{bmap_table_name}\n where code_set_id = {code_set_id}\n and source_code = {source_code}\n and domain_id={domain_id})"""
BK_VIEW_NAME_TEMPLATE = "BKEY_L{src_lvl}_{src_table_name}_{column_name}_L{tgt_lvl}_{domain_id}"
CORE_VIEW_NAME_TEMPLATE = "TXF_CORE_{mapping_name}"
