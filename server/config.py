from collections import namedtuple

smx_path = "/Users/omarnour/Downloads/Production_Citizen_SMX.xlsx"
scripts_path = "/Users/omarnour/Downloads/smx_scripts"

SHEETS = ['stg_tables', 'system', 'data_type', 'bkey', 'bmap'
        , 'bmap_values', 'core_tables', 'column_mapping', 'table_mapping'
        , 'supplements']

LayerDtl = namedtuple("LayerDetail", "level v_db t_db")
LAYERS = {'SRC': LayerDtl(0, 'GDEV1V_STG_ONLINE', 'STG_ONLINE')
    , 'STG': LayerDtl(1, 'GDEV1V_STG', 'GDEV1T_STG')
    , 'TXF_BKEY': LayerDtl(2, 'GDEV1V_INP', '')
    , 'BKEY': LayerDtl(3, 'GDEV1V_UTLFW', 'GDEV1T_UTLFW')
    , 'BMAP': LayerDtl(3, 'GDEV1V_UTLFW', 'GDEV1T_UTLFW')
    , 'SRCI': LayerDtl(4, 'GDEV1V_SRCI', 'GDEV1T_SRCI')
    , 'TXF_CORE': LayerDtl(5, 'GDEV1V_INP', '')
    , 'CORE': LayerDtl(6, 'GDEV1V_BASE', 'GDEV1T_BASE')}

NUMBERS = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
SPECIAL_CHARACTERS = [
    '!', '"', '#', '$', '%', '&', "'", '(', ')', '*', '+', ','
    , '-', '.', '/', ':', ';', '<', '=', '>', '?', '@', '['
    , '\\', ']', '^', '_', '`', '{', '|', '}', '~'
]

cast_dtype_template = """({dtype_name} {precise})"""
col_mapping_template = """{comma}{col_name} {cast_dtype} {alias}"""
from_template = """{schema_name}.{table_name}"""
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
QUERY_TEMPLATE = """ {with_clause}\nselect {distinct}\n{col_mapping}\nfrom {from_clause}\n\t{join_clause}\n{where_clause}\n{group_by_clause}\n{having_clause}"""
# BKEY_IN_QUERY_TEMPLATE = """SELECT {source_key} from {schema_name}.{table_name} where source_key is not null"""
# BKEY_OUT_QUERY_TEMPLATE = """SELECT  """
