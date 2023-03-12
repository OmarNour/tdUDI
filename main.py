from client.smx import *


@time_elapsed_decorator
def start():
    smx = SMX(smx_path, scripts_path)
    smx.parse_file()
    # print(smx.data['test'])
    smx.populate_model(source_name='CSO')
    smx.generate_scripts(source_name=None)


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
