import pandas as pd

from client.smx import *
# from client.smx2 import *


@time_elapsed_decorator
def start(source_name: str|list|None, with_scripts=True):
    smx = SMX()
    smx.parse_file()
    smx.populate_model(source_name=source_name)

    if with_scripts:
        generate_schemas_ddl(smx)
        generate_scripts(smx)
        generate_metadata_scripts(smx)
        deploy(scripts_path)

    open_folder(smx.current_scripts_path)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    try:
        start(source_name=['cso'], with_scripts=True)
        # start(source_name=[], with_scripts=False)
    except KeyboardInterrupt:
        print("Ops!..")
