from client.smx import *


@time_elapsed_decorator
def start():
    smx = SMX(smx_path, scripts_path)
    smx.parse_file()
    smx.extract_all(source_name='cso')
    smx.generate_scripts(source_name='cso')


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    try:
        start()
        # DataSetType(set_type='xxxx')
        # DataSetType(set_type='xxxx', _override=1)
        #
        # print(DataSetType.get_instance())
    except KeyboardInterrupt:
        print("Ops!..")
