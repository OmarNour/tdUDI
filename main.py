from functions.model import *


@time_elapsed_decorator
def start():
    smx = SMX("/Users/omarnour/Downloads/Production_Citizen_SMX.xlsx")
    smx.parse_file()
    smx.extract_all()

    print('DataSetType count:', len(DataSetType.get_instance()))
    print('Layer count:', len(Layer.get_instance()))
    print('Schema count:', len(Schema.get_instance()))
    print('DataType count:', len(DataType.get_instance()))
    print('DataSource count:', len(DataSource.get_instance()))
    print('Table count:', len(Table.get_instance()))
    print('LayerTable count:', len(LayerTable.get_instance()))
    print('DataSet count:', len(DataSet.get_instance()))
    print('Domain count:', len(Domain.get_instance()))
    print('DomainValue count:', len(DomainValue.get_instance()))
    print('Column count:', len(Column.get_instance()))

    for key in Schema.get_instance().keys():
        print('s key:', key, 'id:', Schema.get_instance(_key=key).id)
        tables = Schema.get_instance(_key=key).tables

        table:Table
        for table in tables:
            print(table.schema.schema_name, table.table_name, table.ddl)

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
