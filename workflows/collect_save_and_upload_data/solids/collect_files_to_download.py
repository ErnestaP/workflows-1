import os
from dagster import solid, DynamicOutputDefinition, DynamicOutput

from workflows.utils.generators import generate_mapping_key


# Passing Nothing type variable:
# Solids create_dummy_dir and collect_files_to_download are not depending on each other, because
# create_dummy_dir doesn't return any output which is needed for collect_files_to_download solid.
# In this way solids are ran parallel. However, we want to have dummy files created first, so
# we passing Nothing as an output from create_dummy_dir to collect_files_to_download to set the order of solids.

# collecting files from local dir instead of FTP server.
@solid(output_defs=[DynamicOutputDefinition(dict)])
def collect_files_to_download(context) -> list:
    """
    Collects all the files in under the 'ftp_folder' folder.
    Files starting with a dot (.) are omitted.
    :ftp FTPHost:
    :return: list of all found file's path
    """

    # make sure you're in root dir
    current_dir = os.getcwd()
    os.chdir(current_dir)

    files = [file for file in os.listdir(current_dir) if os.path.isfile(file)]
    for file in files:
        if file.startswith('.'):
            continue
        full_path = os.path.join(current_dir, file)
        if file.endswith('.zip') or file == 'go.xml':
            yield DynamicOutput(value={'file_path': full_path},
                                mapping_key=generate_mapping_key())
        else:
            context.log.warning(f'File with invalid extension on FTP path={full_path}')
