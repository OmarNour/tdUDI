import unittest
from functions.model import *


class MyTestCase(unittest.TestCase):
    # def test_something(self):
    #     self.assertEqual(True, False)  # add assertion here

    @time_elapsed_decorator
    def test_smx01(self):
        smx = SMX("/Users/omarnour/Downloads/Production_Citizen_SMX.xlsx")
        smx.parse_file()
        smx.extract_all()

        assert len(Column.get_instance()) > 0
        print('Schema count:', len(Schema.get_instance()))
        print('DataSource count:', len(DataSource.get_instance()))
        print('Table count:', len(Table.get_instance()))
        print('DataSetType count:', len(DataSetType.get_instance()))
        print('DataSet count:', len(DataSet.get_instance()))
        print('Domain count:', len(Domain.get_instance()))
        print('DomainValue count:', len(DomainValue.get_instance()))
        print('DataType count:', len(DataType.get_instance()))
        print('Column count:', len(Column.get_instance()))


if __name__ == '__main__':
    unittest.main()
