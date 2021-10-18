import os
from shutil import copyfile
from dagster import solid, InputDefinition
from time import localtime, strftime

from workflows.constants import LOCAL_FOLDER_FOR_DOWNLOADED_FILES


# just simulating it. Here we moving files from current dir to downloaded dir
@solid(input_defs=[InputDefinition(name="file_object", dagster_type=dict)])
def download_a_file_from_ftp(context, file_object):
    new_file_name = None
    # come back to root dir, because file which will be downloaded have absolute path
    current_dir = os.getcwd()
    os.chdir(current_dir)
    # where downloaded files will be saved
    target_folder = os.path.abspath(os.path.join(os.getcwd(), LOCAL_FOLDER_FOR_DOWNLOADED_FILES))
    filename_prefix = strftime('%Y-%m-%d_%H:%M:%S', localtime())
    new_file_name = '%s_%s' % (filename_prefix, os.path.basename(file_object['file_path']))
    local_filename = os.path.join(target_folder, new_file_name)

    dir_name = os.path.dirname(file_object['file_path'])
    # moving file
    copyfile(file_object['file_path'], local_filename)
    context.log.info(f'File {new_file_name} with path {dir_name} to {target_folder}')
    return local_filename


    