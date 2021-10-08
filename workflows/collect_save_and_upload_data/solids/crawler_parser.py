import os
from dagster import solid, InputDefinition, String
from scrapy.selector import Selector
from hepcrawl.spiders.oup_spider import OxfordUniversityPressSpider

from workflows.utils.create_dir import create_dir
from workflows.constants import JSONS


# Don't foget to set export SCOAP_DEFAULT_LOCATION= to JSONS folder

class Test:
    meta = {
        "pdf_url": '',
    }

@solid(input_defs=[InputDefinition(name="file_path_in_s3", dagster_type=String)])
def crawler_parser(context, file_path_in_s3):
    file_name = os.path.basename(file_path_in_s3)
    dir_path = os.path.join(os.getcwd(), 'downloaded_from_s3')
    file_full_path = os.path.join(dir_path, file_name)
    cwd = os.getcwd()
    jsons_dir = create_dir(context, cwd, JSONS)
    if jsons_dir:
        f = open(file_full_path, "r")
        selector = Selector(text=f.read(), type='xml')
        # spider = OxfordUniversityPressSpider(target_folder=jsons_dir)
        # a = spider.parse_node(Test(), selector)

    # context.log.info(f'pseudo parsing file {str(a)}')
    return file_full_path
