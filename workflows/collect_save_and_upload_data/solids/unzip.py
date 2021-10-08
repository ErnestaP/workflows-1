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
    file_name = os.path.basename(full_path)
    filename_prefix = strftime('%Y-%m-%d_%H:%M:%S', localtime()) ## not working

    if dir_is_created:
        with zipfile.ZipFile(file_path, 'r') as zip_ref:
            # zipinfos = zip_ref.infolist()
            # for one_file in zipinfos:
            #     one_file.filename = f'{filename_prefix}_{file_name}'
            try:
                zip_ref.extractall(full_path)
                context.log.info(f'file {file_path} is extracted successfully')
            except Exception as e:
                context.log.info(f'Error while extracting the file {file_path}: {e}')
    else:
        pass
