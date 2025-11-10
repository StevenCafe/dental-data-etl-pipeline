from pandas import DataFrame
import pandas as pd
from datetime import date
import logging
import re

from xlsx_interface_extractor import IXlsxExtractor
from utils.utils import convert_minguo_date, clean_numeric_to_str, convert_numeric_to_int

logger = logging.getLogger(__name__)

class Xlsx5Extractor(IXlsxExtractor):
    TABLE_NAME = "期間初複診明細表"
    # File name: 5.期間初複診明細表/期間初複診明細表_1140401_1140401.xls
    FILE_PATTERN = r"5.期間初複診明細表/期間初複診明細表_(\d{7})_(\d{7})\.xls$"
    start_date: date = None
    end_date: date = None

    def is_valid_name(self, file_name):
        pattern = re.compile(self.FILE_PATTERN)
        match = pattern.search(file_name)
        if match:
            text1, text2 = match.groups()
            self.start_date = convert_minguo_date(text1)
            self.end_date = convert_minguo_date(text2)
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
            
            str_fields = ["病歷號","姓名","性別","預約日","看診日","醫師","班別","部門","預約項目","預約治療項目","自費關懷","指定醫師","保險別","初複診","來源","到診否","未到診原因","生日","電話","地址","提醒方式","專案編號","當日成交否"]
            int_fields = ["本次應收"]
            df_all[str_fields] = df_all[str_fields].map(clean_numeric_to_str)
            df_all[int_fields] = df_all[int_fields].map(convert_numeric_to_int)
            return df_all
        except Exception as e:
            err_msg = f"Error in transform: {e}"
            logger.error(err_msg, exc_info=True)
        finally:
            logger.info("Ending func transform")
