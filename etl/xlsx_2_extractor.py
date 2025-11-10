from pandas import DataFrame
import pandas as pd
from datetime import date
import logging
import re

from xlsx_interface_extractor import IXlsxExtractor
from utils.utils import convert_minguo_date, clean_numeric_to_str

logger = logging.getLogger(__name__)

class Xlsx2Extractor(IXlsxExtractor):
    TABLE_NAME = "全日總預約表"
    FILE_PATTERN = r"2.全日預約表/全日總預約表_(\d{7})\.xls$"
    start_date: date = None
    end_date: date = None

    def is_valid_name(self, file_name):
        pattern = re.compile(self.FILE_PATTERN)
        match = pattern.search(file_name)
        if match:
            text = match.groups()[0]
            self.start_date = convert_minguo_date(text)
            return True
        return False

    def transform(self, bucket_name, file_name) -> DataFrame:
        logger.info(f"Starting func transform to {file_name}")

        try:
            # GCS path to your .xls file
            gcs_path = f"gs://{bucket_name}/{file_name}"
            # Read Excel file directly from GCS
            xls = pd.ExcelFile(gcs_path, storage_options={"token": "cloud"}, engine="xlrd")
            # Get all sheet names
            all_sheets = xls.sheet_names
            logger.info("scan all sheets:%s", ", ".join(all_sheets))
            # Read and concatenate all sheets
            df_all = pd.concat(
                [xls.parse(sheet_name) for sheet_name in all_sheets],
                ignore_index=True
            )
            df_all = df_all.map(clean_numeric_to_str)
            return df_all
        except Exception as e:
            err_msg = f"Error in transform: {e}"
            logger.error(err_msg, exc_info=True)
        finally:
            logger.info("Ending func transform")
