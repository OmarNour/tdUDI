from functions.model import *


@time_elapsed_decorator
def start():
    smx = SMX("/Users/omarnour/Downloads/Production_Citizen_SMX.xlsx")
    smx.parse_file()
    smx.extract_all()

    print('Layer count:', len(Layer.get_instance()))
    print('Schema count:', len(Schema.get_instance()))
    print('DataSource count:', len(DataSource.get_instance()))
    print('Table count:', len(Table.get_instance()))
    print('LayerTable count:', len(LayerTable.get_instance()))
    print('DataSetType count:', len(DataSetType.get_instance()))
    print('DataSet count:', len(DataSet.get_instance()))
    print('Domain count:', len(Domain.get_instance()))
    print('DomainValue count:', len(DomainValue.get_instance()))
    print('DataType count:', len(DataType.get_instance()))
    print('Column count:', len(Column.get_instance()))


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    start()
