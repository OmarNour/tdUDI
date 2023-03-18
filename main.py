import pandas as pd

from client.smx import *


@time_elapsed_decorator
def start(source_name: str|None, with_scripts=True):
    smx = SMX(smx_path, scripts_path)
    smx.parse_file()
    smx.populate_model(source_name=source_name)

    generate_scripts(smx) if with_scripts else None

    open_folder(smx.current_scripts_path)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    try:
        start(source_name='cso')
    except KeyboardInterrupt:
        print("Ops!..")
