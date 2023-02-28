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
            _split_0 = _split[0]
            _split_1 = _split[1]

            table__alias = merge_multiple_spaces(_split_0).split(' ', 1)
            table_name = table__alias[0]
            table_alias = table__alias[1] if len(table__alias)>=2 else ''
            print(f"table name {table_name}, alias {table_alias}")

            _split = _split_1.split(' join ', 1)
            _split_0 = _split[0]
            if _split_0.endswith(' left'):
                _split_0 = _split[0].removesuffix(' left')
                new_input_join = 'left join '
            elif _split_0.endswith(' full'):
                _split_0 = _split[0].removesuffix(' full')
                new_input_join = 'full join '
            elif _split_0.endswith(' right'):
                _split_0 = _split[0].removesuffix(' right')
                new_input_join = 'right join '

            print(f"joined on {_split_0}")
            print("===-=-=-=-=-=-=-=-==-=-=-====")
            if len(_split) >= 2:
                parse_join(new_input_join + _split[1])


    # then no split done
    print("end of txt!")
    # return _join_txt


if __name__ == '__main__':
    x = """   INNER JOIN DBSS_CRM_TRANSACTIONSTRANSACTION  xc ON DBSS_CRM_TRANSACTIONSTRANSACTION.ID = P_EDW_TMP_TDEV.JSON_SALES_STG .TRANSACTION_ID
inner OUTER JOIN DBSS_OM_ORDEREDCONTRACTSORDEREDCONTRACT  ON DBSS_OM_ORDEREDCONTRACTSORDEREDCONTRACT.CONFIRMATION_CODE = P_EDW_TMP_TDEV.JSON_SALES_STG .CONFIRMATION_CODE
left JOIN DBSS_OM_ORDEREDCONTRACTSORDEREDDEVICE on DBSS_OM_ORDEREDCONTRACTSORDEREDDEVICE.CONTRACT_ID=DBSS_OM_ORDEREDCONTRACTSORDEREDCONTRACT.ID
inner join DBSS_PC_PRODUCTSITEMVARIANT on DBSS_PC_PRODUCTSITEMVARIANT.id=DBSS_OM_ORDEREDCONTRACTSORDEREDDEVICE.type_id
inner join   DBSS_CRM_TRANSACTIONSPAYMENT on DBSS_CRM_ALYSSASALESINVOICE.transaction_id=DBSS_CRM_TRANSACTIONSPAYMENT.transaction_id

    """
    # x = '('
    y = parse_join(x)
    # print(y)
    # x = 'asd asda fsdf dfggggg and ON'
    # print(split_text(x, '('))

    # y =split_text(merge_multiple_spaces(x), 'join', 1)
    # print(y)
