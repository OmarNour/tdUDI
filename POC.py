import sqlparse

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
for token in parsed_query.tokens:
    print(token)
    print("--------")