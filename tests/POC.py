import sqlparse
from server.functions import *

q = """
select distinct  l.id layer_id, t.id table_id, 1 active
from edw_config."tables" t, edw_config.layers l
where exists (select 1
				from edw_config."schemas" s
				where t.schema_id = s.id
				and  (
						(s.schema_name='stg' and l.abbrev = 'stg')
						or
						(s.schema_name='public' and l.abbrev = 'src' )
				)
			);
"""

parsed_query = sqlparse.parse(q)[0]


# Print the parsed query tokens
# for token in parsed_query.tokens:
#     print(token)
#     print("--------")


def parse_join(join_txt: str):
    _join_txt = ' ' + merge_multiple_spaces(join_txt) + ' '
    _join_txt = _join_txt.lower().replace(' inner ', ' ').replace(' outer ', ' ').strip()
    assert ' from ' not in _join_txt, 'Query cannot be found in join!'
    new_input_join = 'join '

    _split = _join_txt.split(' ', 1)
    if len(_split) >= 2:
        print("===-=-=-=-=-=-=-=-==-=-=-====")
        if _split[0].lower() == 'join':
            print('inner join found')
        elif _split[0].lower() == 'left':
            print('left join found')
        elif _split[0].lower() == 'right':
            print('right join found')
        elif _split[0].lower() == 'full':
            print('full outer join found')

        _split = _split[1].split(' on ', 1)
        print('on split: ', _split)
        if len(_split) >= 2:
            _split_0 = (' ' + _split[0] + ' ').replace(' join ', '')
            _split_1 = _split[1]

            table__alias = merge_multiple_spaces(_split_0).split(' ', 1)
            table_name = table__alias[0]
            table_alias = table__alias[1] if len(table__alias) >= 2 else ''
            print(f"table name {table_name}, alias {table_alias}")

            _split = _split_1.split(' join ', 1)
            join_on = _split[0]
            if join_on.endswith(' left'):
                join_on = join_on.removesuffix(' left')
                new_input_join = 'left join '
            elif join_on.endswith(' full'):
                join_on = join_on.removesuffix(' full')
                new_input_join = 'full join '
            elif join_on.endswith(' right'):
                join_on = join_on.removesuffix(' right')
                new_input_join = 'right join '

            print(f"joined on {join_on}")
            print("===-=-=-=-=-=-=-=-==-=-=-====")
            if len(_split) >= 2:
                parse_join(new_input_join + _split[1])

    # then no split done
    print("end of txt!")
    # return _join_txt


def filter_df():
    # create a dataframe
    df = pd.DataFrame({'name': ['Alice', 'Bob', 'Charlie', 'David'],
                       'age': [25, 30, 35, 40],
                       'gender': ['F', 'M', 'M', 'M']})

    # create a boolean mask for the condition
    mask = df['age'] > 30
    value = None
    mask = (df['name'] == value or value is None)

    # apply the boolean mask to filter the dataframe
    filtered_df = df[mask]

    print(filtered_df)


def replace_():
    sorted_words = ['FACTORY_CREATION_NUMBER', 'convert_table_header', 'RELIGION_DESCRIPTION', 'DATA_EXTRACTION_DATE', 'RELIGION_DESCRIPTION', 'DATA_EXTRACTION_DATE', 'DATA_EXTRACTION_DATE', 'BM_MARITAL_STATUS_ID', 'current_timestampar', 'character_lengthar', 'timezone_minutear',
                    'MODIFICATION_TYPE',
                    'MODIFICATION_TYPE', 'INVALID_DATE_TIME', 'MARITAL_STATUS_ID', 'MODIFICATION_TYPE', 'regr_interceptar', 'deterministicar', 'authorizationar', 'timezone_hourar', 'transactiontime', 'sqlexceptionar', 'octet_lengthar', 'current_rolear', 'current_userar', 'percent_rankar',
                    'current_datear',
                    'width_bucketar', 'current_timear', 'IS_TRANSFERRED', 'BM_RELIGION_ID', 'IS_TRANSFERRED', 'BM_RELIGION_ID', 'DELIVERED_DATE', 'IS_TRANSFERRED', 'stddev_sampar', 'until_changed', 'translate_chk', 'constructoran', 'datablocksize', 'transactionan', 'udtcastlparen', 'char_lengthar',
                    'referencingar', 'CSO_RELIGION.', 'until_closed', 'regr_slopear', 'stddev_popar', 'charactersan', 'sqlwarningar', 'inconsistent', 'row_numberar', 'deallocatear', 'abortsession', 'variant_type', 'referencesar', 'regr_countar', 'constraintar', 'covar_sampar', 'privilegesan',
                    'normalize ar',
                    'casespecific', 'cso_religion', 'characterar', 'parameterar', 'replication', 'temporaryan', 'regr_avgxar', 'errortables', 'covar_popar', 'substringar', 'monresource', 'recursivear', 'translatear', 'statementan', 'generatedan', 'transforman', 'timestampar', 'access_lock',
                    'collationan',
                    'setsessrate', 'nontemporal', 'replcontrol', 'rollforward', 'immediatean', 'intersectar', 'mcharacters', 'regr_avgyar', 'precisionar', 'procedurear', 'integerdate', 'char2hexint', 'RELIGION_ID', 'RELIGION_ID', 'ST_Geometry', 'regr_sxxar', 'modifiesar', 'coalescear', 'fastexport',
                    'smallintar',
                    'externalar', 'multisetar', 'rollbackar', 'specificar', 'checkpoint', 'identityar', 'regr_syyar', 'statistics', 'deferredan', 'regr_sxyar', 'zeroifnull', 'nullifzero', 'revalidate', 'containsan', 'positionar', 'groupingar', 'diagnostic', 'distinctar', 'instancean', 'relativean',
                    'hashbakamp',
                    'vargraphic', 'setresrate', 'add_months', 'var_sampar', 'subscriber', 'hashbucket', 'languagear', 'resignalar', 'trailingar', 'errorfiles', 'intervalar', 'overlapsar', 'preservean', 'protection', 'orderingan', 'functionar', 'monsession', 'continuean', 'CSO_NUMBER', 'ISSUE_DATE',
                    'udtcastas',
                    'string_cs', 'undefined', 'handlerar', 'extractar', 'varyingar', 'preparear', 'connectar', 'leadingar', 'sessionan', 'dynamicar', 'declarear', 'qualified', 'currentar', 'betweenar', 'aggregate', 'ctcontrol', 'uescapear', 'naturalar', 'defaultar', 'threshold', 'expanding',
                    'old_table',
                    'numericar', 'validtime', 'timestamp', 'releasear', 'insteadan', 'permanent', 'executear', 'restartan', 'foreignar', 'triggerar', 'iteratear', 'collectar', 'terminate', 'new_table', 'freespace', 'regr_r2ar', 'returnsar', 'udtmethod', 'integerar', 'varcharar', 'withoutar',
                    'locatoran',
                    'decimalar', 'arglparen', 'var_popar', 'primaryar', 'uppercase', 'cso_card.', 'TIMESTAMP', 'coalesce', 'sampleid', 'dateform', 'volatile', 'objectan', 'oreplace', 'atomicar', 'stepinfo', 'password', 'bigintar', 'compress', 'disabled', 'createar', 'udtusage', 'cursorar',
                    'exceptar', 'deletear',
                    'selectar', 'insertar', 'secondar', 'rollupar', 'kurtosis', 'optionan', 'scrollar', 'minutear', 'commitar', 'methodar', 'escapear', 'publican', 'database', 'existsar', 'fallback', 'valuesar', 'ansidate', 'trailing', 'uniquear', 'nullifar', 'retrieve', 'havingar', 'domainan',
                    'resultar',
                    'equalsan', 'signalar', 'returnar', 'binaryar', 'override', 'repeatar', 'columnar', 'elseifar', 'beforean', 'initiate', 'revokear', 'quantile', 'updatear', 'doublear', 'BATCH_ID', 'BATCH_ID', 'BATCH_ID', 'cso_card', 'usingar', 'logging', 'readsar', 'journal', 'varbyte',
                    'monthar', 'byteint',
                    'percent', 'untilar', 'udttype', 'summary', 'radians', 'qualify', 'average', 'fetchar', 'hashrow', 'inputan', 'mlinreg', 'locking', 'adminan', 'groupar', 'lowerar', 'floatar', 'wherear', 'msubstr', 'monitor', 'suspend', 'crossar', 'extract', 'rightar', 'foundan', 'cluster',
                    'whilear',
                    'orderar', 'objects', 'inoutar', 'consume', 'range_n', 'closear', 'cyclear', 'grantar', 'tablear', 'comment', 'request', 'maximum', 'unionar', 'mergear', 'innerar', 'countar', 'outerar', 'localar', 'checkar', 'upperar', 'restore', 'sqltext', 'enabled', 'degrees', 'profile',
                    'xmlplan',
                    'replace', 'soundex', 'valuear', 'graphic', 'hashamp', 'firstan', 'explain', 'alterar', 'startup', 'account', 'largear', 'loading', 'beginar', 'leavear', 'startar', 'afteran', 'minimum', 'REF_KEY', 'LOAD_ID', 'REF_KEY', 'LOAD_ID', 'REF_KEY', 'LOAD_ID', 'INTEGER', 'DECIMAL',
                    'VARCHAR', 'userar',
                    'descan', 'timear', 'rankar', 'rename', 'resume', 'elsear', 'bothar', 'corrar', 'tbl_cs', 'cubear', 'hourar', 'thenar', 'nullar', 'rowsar', 'setsan', 'nonear', 'viewan', 'workan', 'realar', 'somear', 'openar', 'blobar', 'mindex', 'datear', 'sample', 'fromar', 'clobar', 'eachar',
                    'modify',
                    'select', 'rolean', 'trimar', 'nextan', 'charar', 'callar', 'sqrtar', 'joinar', 'castar', 'zonean', 'format', 'yearar', 'likear', 'substr', 'undoan', 'whenar', 'typean', 'length', 'leftar', 'case_n', 'expand', 'random', 'nowait', 'intoar', 'rights', 'withar', 'dropar', 'loopar',
                    'overar',
                    'exitan', 'fullar', 'casear', 'execar', 'onlyar', 'BIGINT', 'jarar', 'avgar', 'macro', 'sumar', 'modar', 'oldar', 'trace', 'class', 'chars', 'rowid', 'limit', 'acosh', 'anyar', 'endar', 'getar', 'intar', 'mapan', 'sqlar', 'andar', 'atan2', 'logon', 'decar', 'atanh', 'expar',
                    'allar', 'addan',
                    'outar', 'title', 'maxar', 'rowar', 'bytes', 'setar', 'right', 'error', 'forar', 'notar', 'mdiff', 'ascan', 'dayar', 'asinh', 'mload', 'newar', 'minus', 'spool', 'abort', 'index', 'named', 'minar', 'keyan', 'queue', 'absar', 'cast', 'lnar', 'then', 'year', 'byte', 'acos', 'byar',
                    'msum',
                    'cosh', 'asar', 'date', 'sinh', 'show', 'atar', 'mode', 'null', 'csum', 'echo', 'toar', 'atan', 'else', 'trim', 'ifar', 'lock', 'skew', 'perm', 'isar', 'asin', 'dump', 'noar', 'case', 'help', 'mavg', 'ofar', 'hash', 'left', 'when', 'dual', 'long', 'inar', 'orar', 'onar', 'tanh',
                    'from', 'doar',
                    'give', 'DATE', 'TIME', 'CHAR', 'CLOB', 'cos', 'sel', 'ins', 'sin', 'tan', 'upd', 'top', 'not', 'but', 'ave', 'off', 'amp', 'end', 'del', 'log', 'and', 'ret', 'uc', 'cs', 'bt', 'ge', 'or', 'ne', 'ct', 'on', 'in', 'cm', 'cv', 'lt', 'et', 'gt', 'is', 'le', 'eq', 'ss', 'cd', 'as',
                    '', '']

    _trx = "cso_new_person.cso_number=cso_card.cso_number"
    replace_ch = ''
    single_quotes_pattern = r"'[^']*'"
    _trx = re.sub(single_quotes_pattern, replace_ch, _trx)
    for word in sorted_words:
        if word.lower() in _trx.lower():
            print(word)
        _trx = _trx.lower().replace(word.lower(), replace_ch).strip()

    print(_trx)


def process_row(row):
    print(row)
    print('asdadasd')
    if row.set_of_numbers:
        print('zzzzzz')


def td_test():
    import teradatasql

    # Connect to the Teradata database
    host = 'localhost'  # Replace with the IP address of your virtual machine
    user = 'dbc'
    password = 'dbc'
    dbc = teradatasql.connect(host=host, user=user, password=password)

    # Execute a SQL query
    query = 'SELECT * FROM stg_online.ngo_ngo_allbanks order by 1 desc'
    with dbc.cursor() as cursor:
        cursor.execute(query)
        results = cursor.fetchall()

    # Print the results
    for row in results:
        print(row)

    # Disconnect from the database
    dbc.close()


def populate_table(add_new_rows: bool):
    from faker import Faker

    # Connect to the Teradata database
    host = 'localhost'  # Replace with the IP address of your virtual machine
    user = 'dbc'
    password = 'dbc'
    dbc = teradatasql.connect(host=host, user=user, password=password)
    cursor = dbc.cursor()

    if add_new_rows:
        # Generate fake data
        fake = Faker()

        # Generate and execute the INSERT statement
        insert_template = """
            INSERT INTO stg_online.ngo_ngo_allbanks (
                PK_BANKID, NAMEARABIC, NAMEENGLISH, PNUSERCODE, DLASTDMLDATE, BATCH_ID,
                REF_KEY, CREATION_TIMESTAMP, UPDATE_TIMESTAMP
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        params = []
        for i in range(1000):
            params.append((
                i + 1,
                fake.name(),
                fake.name(),
                fake.random_int(),
                fake.past_datetime(),
                fake.random_int(),
                fake.random_int(),
                fake.past_datetime(),
                fake.past_datetime()
            ))
        # with dbc.cursor() as cursor:
        cursor.execute("delete from stg_online.ngo_ngo_allbanks;")
        cursor.executemany(insert_template, params)
        print(f"{cursor.rowcount} rows inserted.")

    # Execute the COUNT(*) query
    query = 'SELECT COUNT(*) FROM stg_online.ngo_ngo_allbanks'
    # with dbc.cursor() as cursor:
    cursor.execute(query)
    row = cursor.fetchone()

    # Print the result
    print(f"Number of rows in the table: {row[0]}")

    # Disconnect from the database
    dbc.close()

# def get_database_connection():

def read_df_previous_row():
    import pandas as pd
    df = pd.read_csv('data.csv')
    df['previous_row'] = df.groupby('id')['value'].shift(1)
    df['diff'] = df['value'] - df['previous_row']
    print(df)


def insert_stmt():
    x = """INSERT INTO GDEV1T_GCFR.ETL_PROCESS 
    (SOURCE_NAME, PROCESS_TYPE, PROCESS_NAME, BASE_TABLE, APPLY_TYPE, RECORD_ID, SEQUENCE, active, BKEY_PRTY_DOMAIN_1
    , ALWAYS_RUN, INPUT_VIEW_DB, TARGET_TABLE_DB, TARGET_VIEW_DB, SRCI_TABLE_DB, STG_TABLE_NAME) 
    VALUES ('{ASDA}', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
""".format(ASDA='asdad')

    y = """INSERT INTO GDEV1T_GCFR.ETL_PROCESS 
        (SOURCE_NAME, PROCESS_TYPE, PROCESS_NAME, BASE_TABLE, APPLY_TYPE, RECORD_ID, SEQUENCE, active, BKEY_PRTY_DOMAIN_1
        , ALWAYS_RUN, INPUT_VIEW_DB, TARGET_TABLE_DB, TARGET_VIEW_DB, SRCI_TABLE_DB, STG_TABLE_NAME) 
        VALUES ('%s', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """ % ('adsd')

    print(x)
    print(y)


class XCV:
    run_id = str(generate_run_id())
    raise_if_error = True

    def __init__(self):
        self.run_id_2 = str(generate_run_id())


if __name__ == '__main__':
    print(parse_data_type('varchar(10)'))
    print(parse_data_type('INTEGER'))

    # for col in CORE_TECHNICAL_COLS:
    #     print(col.__repr__())
    # print(col.column_name, col.data_type, col.is_modification_type, col.is_created_at)
    # x = XCV()
    # y = XCV()
    # print(x.run_id, x.run_id_2)
    # print(y.run_id, y.run_id_2)
    # print(XCV.run_id)
    # print(y.raise_if_error)
    # XCV.raise_if_error = False
    # print(x.raise_if_error)
    # insert_stmt()
    # populate_table(add_new_rows=True)
    # td_test()
    # replace_()
    # x = """   left  JOIN TADAMON_GOVERNORATE ON TADAMON_GOVERNORATE.GOVERNORATE_ID=TADAMON_CARDS.GOVERNORATE_ID
    """
    #     x = """
    #      (SEL  beneficiary_national_id, APPLICATION_ID,MEMBER_ID
    #  FROM stg_online.TADAMON_CARDS)A
    #  JOIN stg_online.TADAMON_MEMBERS B
    # ON A.APPLICATION_ID = B.APPLICATION_ID
    #     """
    # y = parse_join(x)
    # print(y)
    # x = 'asd asda fsdf dfggggg and ON'
    # print(split_text(x, '('))

    # y =split_text(merge_multiple_spaces(x), 'join', 1)
    # print(y)
    # filter_df()
    # data = {'set_of_numbers': []}
    # df = pd.DataFrame(data)
    # dfx = df.fillna('')
    # print(dfx.apply(process_row, axis=1) if not dfx.empty else 'aqaqaq')
