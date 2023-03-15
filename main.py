import pandas as pd

from client.smx import *


@time_elapsed_decorator
def start(source_name: str|None):
    smx = SMX(smx_path, scripts_path)
    smx.parse_file()
    smx.populate_model(source_name=source_name)
    # print(smx.source_systems['source_system_name'].values.tolist())
    generate_scripts(smx)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    try:
        start(source_name='cso')
    except KeyboardInterrupt:
        print("Ops!..")
