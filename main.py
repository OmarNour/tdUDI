from client.smx import *


@time_elapsed_decorator
def start(source_name: str|list|None, with_scripts=True, with_deploy=True):
    smx = SMX()
    smx.parse_file()
    smx.populate_model(source_name=source_name)

    if with_scripts:
        # generate_schemas_ddl(smx)
        generate_scripts(smx)
        generate_metadata_scripts(smx)
        if with_deploy:
            deploy()
        # generate_fake_data()

    open_folder(smx.current_scripts_path)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # pipreqs /Users/omarnour/PycharmProjects/tdUDI --force --ignore tests
    try:
        start(source_name=['cso'], with_scripts=True, with_deploy=False)
        # start(source_name=[], with_scripts=False)
        # start(source_name=[], with_scripts=True)
    except KeyboardInterrupt:
        print("Ops!..")
