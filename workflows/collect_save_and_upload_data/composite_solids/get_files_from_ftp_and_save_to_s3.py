from dagster import composite_solid, DynamicOutputDefinition, String, List, OutputDefinition, Output

from workflows.collect_save_and_upload_data.solids.connect_to_ftp_server import connect_to_ftp_server
from workflows.collect_save_and_upload_data.solids.collect_files_to_download import collect_files_to_download
from workflows.collect_save_and_upload_data.solids.uploading_files_to_ftp import uploading_files_to_ftp
from workflows.collect_save_and_upload_data.solids.dowloanda_file_from_ftp import download_a_file_from_ftp
from workflows.collect_save_and_upload_data.solids.uploading_files_to_s3 import uploading_files_to_s3
from workflows.collect_save_and_upload_data.solids.unzip import unzip


@composite_solid(output_defs=[DynamicOutputDefinition(String)])
def get_files_from_ftp_and_save_to_s3():
    ftp = connect_to_ftp_server()
    start = uploading_files_to_ftp(ftp)
    downloaded_files_from_ftp = collect_files_to_download(ftp, start).map(download_a_file_from_ftp)
    unzipped_files_paths = downloaded_files_from_ftp.map(unzip)
    s3_keys = unzipped_files_paths.map(uploading_files_to_s3)
    return s3_keys

