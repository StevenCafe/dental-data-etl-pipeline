from pandas import DataFrame
import pandas as pd
from datetime import datetime, date
import logging
import re
import openpyxl

from xlsx_interface_extractor import IXlsxExtractor
from utils.utils import clean_numeric_to_str, convert_numeric_to_int

logger = logging.getLogger(__name__)

class Xlsx3Extractor(IXlsxExtractor):
    TABLE_NAME = "reviews"
    # File name: 3.Dr right reviews/[Dr.Right] reviews_***牙醫診所01_2025.04.01_2025.04.01_1743556500.0.xlsx
    FILE_PATTERN = r"3.Dr right reviews/\[Dr.Right\] reviews_***牙醫診所.+_([\d.]{10})_([\d.]{10})_.+\.xlsx$"
    start_date: date = None
    end_date: date = None

    def is_valid_name(self, file_name):
        pattern = re.compile(self.FILE_PATTERN)
        match = pattern.search(file_name)
        if match:
            text1, text2 = match.groups()
            self.start_date = datetime.strptime(text1, "%Y.%m.%d")
            self.end_date = datetime.strptime(text2, "%Y.%m.%d")
            return True
        return False

    def transform(self, bucket_name, file_name) -> DataFrame:
        logger.info(f"Starting func transform to {file_name}")

        try:
            # GCS path to your .xls file
            gcs_path = f"gs://{bucket_name}/{file_name}"
            # Read Excel file directly from GCS
            xls = pd.ExcelFile(gcs_path, storage_options={"token": "cloud"}, engine="openpyxl")
            # Get all sheet names
            all_sheets = [sheet for sheet in xls.sheet_names if "All" in sheet]
            logger.info("scan all sheets:%s", ", ".join(all_sheets))
            # Read and concatenate all sheets
            df_all = pd.concat(
                [xls.parse(sheet_name) for sheet_name in all_sheets],
                ignore_index=True
            )
            str_fields = ["姓名","來源","評論內容","回覆內容","處理紀錄","分類","醫師診","狀態","負責人"]
            int_fields = ["星等"]
            df_all[str_fields] = df_all[str_fields].map(clean_numeric_to_str)
            df_all[int_fields] = df_all[int_fields].map(convert_numeric_to_int)
            return df_all
        except Exception as e:
            err_msg = f"Error in transform: {e}"
            logger.error(err_msg, exc_info=True)
        finally:
            logger.info("Ending func transform")
