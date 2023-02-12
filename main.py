from functions.smx import *


@time_elapsed_decorator
def start():
    smx = SMX(smx_path, scripts_path)
    smx.parse_file()
    smx.extract_all()
    smx.generate_scripts()


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
