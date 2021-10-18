from workflows.collect_save_and_upload_data.solids.crawler_parser import crawler_parser
from dagster.core.execution.context.invocation import build_solid_context


# TODO: move to master
def test_crawler_parser():
    keys_of_s3 = ['dummy_files/2021-10-18_12:25:37_oup.xml/ptab104.xml',
                  'dummy_files/super_fake.pdf/fake.pdf',  # folder doesn't exist
                  'dummy_files/2021-10-18_12:25:37_oup.xml/fake.pdf',  # file doesn't exist
                  'dummy_files/2021-10-18_12:25:37_oup.xml/ptab106.xml',
                  'dummy_files/2021-10-18_12:25:37_scoap3.archival/ptab104.pdf',
                  'dummy_files/2021-10-18_12:25:37_scoap3.archival/ptab106.pdf',
                  'dummy_files/2021-10-18_12:25:37_scoap3.pdf/ptab104.pdf',
                  'fake',
                  'dummy_files/2021-10-18_12:25:37_scoap3.pdf/ptab106.pdf']

    context = build_solid_context()
    for key in keys_of_s3:
        crawler_parser(context, key)
