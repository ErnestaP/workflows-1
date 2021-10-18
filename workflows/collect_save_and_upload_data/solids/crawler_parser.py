import os
from dagster import solid, InputDefinition, String
from scrapy.selector import Selector
from hepcrawl.spiders.oup_spider import OxfordUniversityPressSpider
import json

from workflows.utils.create_dir import create_dir
from workflows.constants import JSONS


# TODO: move to master
# Don't foget to set export SCOAP_DEFAULT_LOCATION and HEPCRAWL_BASE_WORKING_DIR  to JSONS folder
class Test:
    meta = {
        "pdf_url": ''}


@solid(input_defs=[InputDefinition(name="key_in_s3", dagster_type=String)])
def crawler_parser(context, key_in_s3):
    file_name = os.path.basename(key_in_s3)
    cwd = os.getcwd()
    grouping_folder = os.path.basename(os.path.dirname(key_in_s3))
    dir_path = os.path.join(cwd, 'downloaded_from_s3', grouping_folder)
    jsons_dir = create_dir(context, cwd, JSONS)

    # for file_name in files_names:
    file_full_path = os.path.join(dir_path, file_name)
    suffix = file_name.split('.')[-1]

    if jsons_dir and suffix=='xml':
        jsons_dir_with_grouping_folder = create_dir(context, cwd, JSONS, grouping_folder)
        jsons_dir_path = os.path.join(cwd, JSONS, grouping_folder)
        if jsons_dir_with_grouping_folder:
            try:  # if file doesn't exist in downloaded_from_s3 folder
                with open(file_full_path, 'r') as file:
                    selector = Selector(text=file.read(), type=suffix)
                    spider = OxfordUniversityPressSpider(target_folder=os.path.join(cwd, JSONS))
                    try:  # if xml file is corrupted
                        json_obj = spider.parse_node(Test(), selector)
                        file_name_with_json_suffix = file_name.replace(suffix, 'json')
                        jsons_full_path = os.path.join(jsons_dir_path, file_name_with_json_suffix)
                        with open(jsons_full_path, 'w') as json_file:
                            parsed = json.loads(str(json_obj).replace("'", '"'))
                            json_file.write(json.dumps(parsed, indent=4, sort_keys=True))
                    except Exception as e:
                        context.log.error(f'ERROR while parsing a file {file_full_path}: {e}')
            except Exception as e:
                context.log.error(f'ERROR while opening  a file {file_full_path}: {e}')
    # return file_full_path
