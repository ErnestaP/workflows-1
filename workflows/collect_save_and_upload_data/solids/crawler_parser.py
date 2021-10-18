import os
from dagster import solid, InputDefinition, String
from scrapy.selector import Selector
from hepcrawl.spiders.oup_spider import OxfordUniversityPressSpider
import json

from workflows.utils.create_dir import create_dir
from workflows.constants import JSONS


# Don't foget to set export SCOAP_DEFAULT_LOCATION and HEPCRAWL_BASE_WORKING_DIR  to JSONS folder
class Test:
    def __inti__(self, zip_filepath, xml_file, pdf_url, pdfa_url):
        self.zip_filepath = zip_filepath
        self.xml_file = xml_file
        self.pdf_url = pdf_url
        self.pdfa_url = pdfa_url

    def __call__(self, *args, **kwargs):
        # meta = {"package_path": self.zip_filepath,
        #         "xml_url": self.xml_file,
        #         "pdf_url": self.pdf_url,
        #         "pdfa_url": self.pdfa_url}
        # )
        meta = {"package_path": '',
                "xml_url": '',
                "pdf_url": '',
                "pdfa_url": ''}
        yield meta


@solid(input_defs=[InputDefinition(name="file_path_in_s3", dagster_type=String)])
def crawler_parser(context, file_path_in_s3):
    file_name = os.path.basename(file_path_in_s3)
    cwd = os.getcwd()
    dir_path = os.path.join(cwd, 'downloaded_from_s3')
    jsons_dir = create_dir(context, cwd, JSONS)
    jsons_dir_path = os.path.join(cwd, JSONS)

    # for file_name in files_names:
    file_full_path = os.path.join(dir_path, file_name)
    context.log.info(file_full_path)
    if jsons_dir:
        with open(file_full_path, 'r') as file:
            suffix = file_name.split('.')[1]
            selector = Selector(text=file.read(), type=suffix)
            spider = OxfordUniversityPressSpider(target_folder=jsons_dir_path)
            json_obj = spider.parse_node(Test(), selector)

            file_name_with_json_suffix = file_name.replace(suffix, 'json')
            jsons_full_path=os.path.join(jsons_dir_path, file_name_with_json_suffix)
            with open(jsons_full_path, 'w') as json_file:
                parsed = json.loads(str(json_obj).replace("'", '"'))
                json_file.write(json.dumps(parsed, indent=4, sort_keys=True))

    # return file_full_path
