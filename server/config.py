from collections import namedtuple

smx_path = "/Users/omarnour/Downloads/Production_Citizen_SMX.xlsx"
scripts_path = "/Users/omarnour/Downloads/smx_scripts"
DB_NAME = 'teradata'
cls_keys = {
        'server': 'server_name'
        , 'DataBaseEngine': 'name'
        , 'datasource': 'source_name'
        , 'schema': ('db_id','schema_name')
        , 'table': ('schema_id', 'table_name')
        , 'DataSetType': 'set_type'
        , 'DataSet': ('set_type_id', 'set_code')
        , 'Domain': ('data_set_id', 'domain_code')
        , 'DomainValue': ('domain_id', 'source_key')
        , 'Column': ('table_id', 'column_name')
        , 'DataType': ('db_id', 'dt_name')
        , 'Layer': 'layer_name'
        , 'LayerTable': ('layer_id', 'table_id')
        , 'Pipeline': ('src_lyr_table_id', 'tgt_lyr_table_id')
        , 'ColumnMapping': ('pipeline_id', 'col_seq', 'tgt_col_id')
        , 'Filter' : ('pipeline_id', 'filter_seq')
        , 'GroupBy' : ('pipeline_id', 'col_id')
    }

SHEETS = ['stg_tables', 'system', 'data_type', 'bkey', 'bmap'
        , 'bmap_values', 'core_tables', 'column_mapping', 'table_mapping'
        , 'supplements']

DS_BKEY = 'BKEY'
DS_BMAP = 'BMAP'
LayerDtl = namedtuple("LayerDetail","type level v_db t_db")
LAYER_TYPES = ['landing', 'staging', 'surrogate', 'srci', 'core']
LAYERS = {'SRC': LayerDtl(0, 0, 'GDEV1V_STG_ONLINE', 'STG_ONLINE')
    , 'STG': LayerDtl(1, 1, 'GDEV1V_STG', 'GDEV1T_STG')
    , 'TXF_BKEY': LayerDtl(1, 2, 'GDEV1V_INP', '')
    , 'BKEY': LayerDtl(2, 3, 'GDEV1V_UTLFW', 'GDEV1T_UTLFW')
    , 'BMAP': LayerDtl(2, 3, 'GDEV1V_UTLFW', 'GDEV1T_UTLFW')
    , 'SRCI': LayerDtl(3, 4, 'GDEV1V_SRCI', 'GDEV1T_SRCI')
    , 'TXF_CORE': LayerDtl(3, 5, 'GDEV1V_INP', '')
    , 'CORE': LayerDtl(4, 6, 'GDEV1V_BASE', 'GDEV1T_BASE')}

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

cast_dtype_template = """({dtype_name} {precise})"""
col_mapping_template = """{comma}{col_name} {cast_dtype} {alias}"""
from_template = """{schema_name}.{table_name} {alias}"""
where_template = """where {conditions}"""
group_by_template = """group by {columns}"""
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
QUERY_TEMPLATE = """ {with_clause}\nselect {distinct}\n{col_mapping}\nfrom {from_clause}\n{join_clause}\n{where_clause}\n{group_by_clause}\n{having_clause}"""
