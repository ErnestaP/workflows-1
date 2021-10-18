import os
from dagster import solid, InputDefinition, String
import zipfile
from time import localtime, strftime

from workflows.utils.create_dir import create_dir
from workflows.constants import UNZIPED_FILES


@solid(input_defs=[InputDefinition(name="file_path", dagster_type=String)])
def unzip(context, file_path):
    cwd = os.getcwd()
    dir_is_created = create_dir(context, cwd, UNZIPED_FILES)
    full_path = os.path.join(cwd, UNZIPED_FILES)
    file_name = os.path.basename(file_path)
    folder_name = file_name.split(".zip")[0]
    path_for_unzipping = os.path.join(full_path, folder_name)

    if dir_is_created:
        try:
            with zipfile.ZipFile(file_path, 'r') as zip_ref:
                try:
                    zip_ref.extractall(path_for_unzipping)
                    context.log.info(f'file {file_path} is extracted successfully')
                except Exception as e:
                    context.log.info(f'Error while extracting the file {file_path}: {e}')
        except Exception as e:
            context.log.error(f'Error while reading the file {file_path}: {e}')

    else:
        pass
