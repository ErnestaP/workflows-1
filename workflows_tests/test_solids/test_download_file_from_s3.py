from workflows.collect_save_and_upload_data.solids.download_file_from_s3 import download_file_from_s3
from dagster.core.execution.context.invocation import build_solid_context


# TODO: move to master
def test_download_file_from_s3():
    keys_of_s3 = ['dummy_files/2021-10-18_12:25:37_oup.xml/ptab104.xml',
                  'dummy_files/super_fake.pdf/fake.pdf',  # folder doesn't exist
                  'dummy_files/2021-10-18_12:25:37_oup.xml/fake.pdf',  # file doesn't exist
                  'dummy_files/2021-10-18_12:25:37_oup.xml/ptab106.xml',
                  'dummy_files/2021-10-18_12:25:37_scoap3.archival/ptab104.pdf',
                  'dummy_files/2021-10-18_12:25:37_scoap3.archival/ptab106.pdf',
                  'dummy_files/2021-10-18_12:25:37_scoap3.pdf/ptab104.pdf',
                  'fake',
                  'dummy_files/2021-10-18_12:25:37_scoap3.pdf/ptab106.pdf']

    context = build_solid_context(resources={
        "aws": {
            "aws_access_key_id": "84bdbd7c1c05477eaf6993439fc73953",
            "aws_secret_access_key": "7033cee4acc349058756b34698fa8068",
            "endpoint_url": "https://s3.cern.ch",
            "raw_files_bucket": "scoap3-test-ernesta",
            "modified_files_bucket": "",
        }
    })
    for key in keys_of_s3:
        s3_key = download_file_from_s3(context, key)
        assert key == s3_key
