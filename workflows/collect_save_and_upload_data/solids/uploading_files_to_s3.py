import os
from dagster import solid, InputDefinition, Nothing, String, DynamicOutputDefinition, DynamicOutput
import boto3

from workflows.utils.generators import generate_mapping_key
from workflows.constants import DUMMY_FILES_SUB_KEY, UNZIPED_FILES


# TODO: move to master. Closely check before doing it!!!
@solid(required_resource_keys={"aws"},
       input_defs=[InputDefinition("start", Nothing)],
       output_defs=[DynamicOutputDefinition(String)])
def uploading_files_to_s3(context):
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
    path_to_files = os.path.join(pwd, UNZIPED_FILES)

    for dir_name in os.listdir(path_to_files):
        if os.path.isdir(os.path.join(path_to_files, dir_name)):  ## need to add this fix to master
            dir_full_path = os.path.join(path_to_files, dir_name)
            file_names = [file_name for file_name in os.listdir(dir_full_path) if
                          os.path.isfile(os.path.join(dir_full_path, file_name))]

            for file_name in file_names:
                path_to_file = os.path.join(pwd, f'{UNZIPED_FILES}/{dir_name}/{file_name}')
                key = f"{DUMMY_FILES_SUB_KEY}/{dir_name}/{file_name}"

                # Already uploaded with dir name. Uncomment in prod
                # s3_resource.Bucket(bucket_name).put_object(
                #     Key=key,
                #     Body=open(path_to_file, 'rb')
                # )
    # return keys of files, where they were uploaded.
    # Because after this step we will use just Keys of s3 to re-download files to modify them
    for key in s3_client.list_objects(Bucket=bucket_name)['Contents']:
        context.log.error(str(key['Key']))
        # keys, which ended with '/' are dirs, we need just files
        # files, which we want to download from s3
        if not key['Key'].endswith('/') and f'{DUMMY_FILES_SUB_KEY}/' in key['Key']:
            yield DynamicOutput(value=key['Key'],
                                mapping_key=generate_mapping_key())
