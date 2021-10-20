import os
from dagster import solid, InputDefinition, String, Failure, EventMetadata, OutputDefinition, Output
import zipfile

from workflows.utils.create_dir import create_dir
from workflows.constants import UNZIPPED_FILES
from workflows.utils.generators import generate_mapping_key


@solid(input_defs=[InputDefinition(name="file_path", dagster_type=String)],
       output_defs=[OutputDefinition(String)])
def unzip(context, file_path):
    cwd = os.getcwd()
    dir_is_created = create_dir(context, cwd, UNZIPPED_FILES)
    file_name = os.path.basename(file_path)
    grouping_folder = file_name.split(".zip")[0]
    path_for_unzipped_files = os.path.join(cwd, UNZIPPED_FILES, grouping_folder)

    if dir_is_created:
        try:
            with zipfile.ZipFile(file_path, 'r') as zip_ref:
                try:
                    zip_ref.extractall(path_for_unzipped_files)
                    context.log.info(f'file {file_path} is extracted successfully')
                    yield Output(path_for_unzipped_files)
                except Exception as e:
                    context.log.info(f'Error while extracting the file {file_path}: {e}')
        except Exception as e:
            context.log.error(f'Error while reading the file {file_path}: {e}')
    else:
        raise Failure(
            description=f"{UNZIPPED_FILES} is not created!",
            metadata={
                "filepath": EventMetadata.path(path_for_unzipped_files),
            },
        )
