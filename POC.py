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
            _split_0 = (' ' +_split[0]+' ').replace(' join ','')
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


if __name__ == '__main__':
    x = """   left  JOIN TADAMON_GOVERNORATE ON TADAMON_GOVERNORATE.GOVERNORATE_ID=TADAMON_CARDS.GOVERNORATE_ID
    """
    #     x = """
    #      (SEL  beneficiary_national_id, APPLICATION_ID,MEMBER_ID
    #  FROM stg_online.TADAMON_CARDS)A
    #  JOIN stg_online.TADAMON_MEMBERS B
    # ON A.APPLICATION_ID = B.APPLICATION_ID
    #     """
    y = parse_join(x)
    # print(y)
    # x = 'asd asda fsdf dfggggg and ON'
    # print(split_text(x, '('))

    # y =split_text(merge_multiple_spaces(x), 'join', 1)
    # print(y)
