from functions.smx import *


@time_elapsed_decorator
def start():
    smx_path = "/Users/omarnour/Downloads/Production_Citizen_SMX.xlsx"
    scripts_path = "/Users/omarnour/Downloads/smx_scripts"
    smx = SMX(smx_path, scripts_path)
    smx.parse_file()
    smx.extract_all()
    smx.generate_scripts()



    # for key in Schema.get_instance().keys():
    #     print('s key:', key, 'id:', Schema.get_instance(_key=key).id)
    #     tables = Schema.get_instance(_key=key).tables
    #
    #     table:Table
    #     for table in tables:
    #         if table.ddl:
    #             print(table.schema.schema_name, table.table_name, table.ddl)

    # s = Schema.get_instance(_key='gdev1t_stg')
    # print(s.tables)
    # for key in Table.get_instance().keys():
    #     print("key: ", key)
    # for table in Schema.get_instance(_key='gdev1t_stg').tables:
    #     print(table.table_name)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    try:
        start()
    except KeyboardInterrupt:
        print("Ops!..")
