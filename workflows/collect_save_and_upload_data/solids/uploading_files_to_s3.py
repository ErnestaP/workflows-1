import os
from dagster import solid, InputDefinition, String, DynamicOutputDefinition, DynamicOutput, List, Dict, OutputDefinition
import boto3

from workflows.utils.generators import generate_mapping_key
from workflows.constants import DUMMY_FILES_SUB_KEY, UNZIPED_FILES


@solid(required_resource_keys={"aws"},
       input_defs=[InputDefinition("unzipped_folder_path", String)],
       output_defs=[DynamicOutputDefinition(String)])
def uploading_files_to_s3(context, unzipped_folder_path):
    aws_access_key_id = context.resources.aws["aws_access_key_id"]
    aws_secret_access_key = context.resources.aws["aws_secret_access_key"]
    endpoint_url = context.resources.aws["endpoint_url"]

    s3_client = boto3.client(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        endpoint_url=endpoint_url
    )
    s3_resource = boto3.resource(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        endpoint_url=endpoint_url
    )

    bucket_name = context.resources.aws["raw_files_bucket"]

    pwd = os.getcwd()
    grouping_folder = os.path.basename(unzipped_folder_path)

    file_names = [file_name for file_name in os.listdir(unzipped_folder_path) if
                  os.path.isfile(os.path.join(unzipped_folder_path, file_name))]
    for file_name in file_names:
        path_to_file = os.path.join(pwd, f'{UNZIPED_FILES}/{grouping_folder}/{file_name}')
        key = f"{DUMMY_FILES_SUB_KEY}/{grouping_folder}/{file_name}"
        # s3_resource.Bucket(bucket_name).put_object(
        #     Key=key,
        #     Body=open(path_to_file, 'rb')
        # )
        yield DynamicOutput(value=key, mapping_key=generate_mapping_key())
    # return  DynamicOutput('aaa'
