import functions_framework
import logging

import config.logging_config as logging_config
from etl.xlsx_interface_extractor import IXlsxExtractor
from etl.xlsx_1_extractor import Xlsx1Extractor
from etl.xlsx_2_extractor import Xlsx2Extractor
from etl.xlsx_3_extractor import Xlsx3Extractor
from etl.xlsx_4_extractor import Xlsx4Extractor
from etl.xlsx_5_extractor import Xlsx5Extractor
from etl.xlsx_6_extractor import Xlsx6Extractor
from etl.xlsx_7_extractor import Xlsx7Extractor
from etl.bigquery_loader import BigQueryLoader

BUCKET = "y***tt*s***d"
PROJECT = "valid-bedrock-455402-***d6"
DATASET = "raw_dental_treatments"

logger = logging.getLogger(__name__)
logger.info("Starting func main")

def get_xlsx_handler(name) -> IXlsxExtractor:
    xlsx_factory: list[IXlsxExtractor] = [
        Xlsx1Extractor(),
        Xlsx2Extractor(),
        Xlsx3Extractor(),
        Xlsx4Extractor(),
        Xlsx5Extractor(),
        Xlsx6Extractor(),
        Xlsx7Extractor(),
        ]
    for x in xlsx_factory:
        if x.is_valid_name(name):
            return x
    return None

# Triggered by a change in a storage bucket
@functions_framework.cloud_event
def hello_gcs(cloud_event):
    data = cloud_event.data

    event_id = cloud_event["id"]
    event_type = cloud_event["type"]
    bucket = data["bucket"]
    name = data["name"]
    metageneration = data["metageneration"]
    timeCreated = data["timeCreated"]
    updated = data["updated"]

    logger.info(f"Bucket: {bucket}, File: {name}, Event ID: {event_id}, Event type: {event_type}, Metageneration: {metageneration}, Created: {timeCreated}, Updated: {updated}")

    xlsx_extractor = get_xlsx_handler(name)
    if xlsx_extractor is None:
        logger.error(f"Unmatched file name:{name}")
    else:
        df = xlsx_extractor.transform(BUCKET, name)
        bq_loader = BigQueryLoader()
        bq_loader.load_to_bq_by_partition_date(df, f"{PROJECT}.{DATASET}.{xlsx_extractor.TABLE_NAME}", xlsx_extractor.start_date.strftime('%Y%m%d'))

logger.info("Ending func main")
