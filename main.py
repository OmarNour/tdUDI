import pandas as pd

from client.smx import *


@time_elapsed_decorator
def start(source_name: str|list|None, with_scripts=True):
    smx = SMX(smx_path, scripts_path)
    smx.parse_file()
    smx.populate_model(source_name=source_name)

    if with_scripts:
        generate_schemas_ddl(smx)
        generate_scripts(smx)
        generate_metadata_scripts(smx)

    open_folder(smx.current_scripts_path)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    try:
        start(source_name='ngo')
    except KeyboardInterrupt:
        print("Ops!..")
