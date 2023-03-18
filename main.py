import pandas as pd

from client.smx import *


@time_elapsed_decorator
def start(source_name: str|None):
    smx = SMX(smx_path, scripts_path)
    smx.parse_file()
    smx.populate_model(source_name=source_name)

    generate_scripts(smx)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    try:
        start(source_name='cso')
        # xls = pd.read_excel(smx_path, sheet_name="STG tables")
        # print(xls)
    except KeyboardInterrupt:
        print("Ops!..")
