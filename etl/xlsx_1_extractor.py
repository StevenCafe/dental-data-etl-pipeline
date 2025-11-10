from pandas import DataFrame
import pandas as pd
from datetime import date
import logging
import re

from xlsx_interface_extractor import IXlsxExtractor
from utils.utils import convert_minguo_date, clean_numeric_to_str, convert_numeric_to_int

logger = logging.getLogger(__name__)

class Xlsx1Extractor(IXlsxExtractor):
    TABLE_NAME = "治療計劃明細表"
    FILE_PATTERN = r"1.治療計劃明細表/治療計劃明細表_分醫師_(\d{7})_(\d{7})\.xls$"
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
            all_sheets = [sheet for sheet in xls.sheet_names if "總表" in sheet]
            logger.info("scan all sheets:%s", ", ".join(all_sheets))
            # Read and concatenate all sheets
            df_all = pd.concat(
                [xls.parse(sheet_name) for sheet_name in all_sheets],
                ignore_index=True
            )
            # Define the parent-record fields
            parent_fields = ["專案編號", "專案日期", "成交日", "執行日", "方案類別", "病歷號碼", "姓名", "性別", "年齡", "醫師", "諮詢師", "協同諮詢師", "電話關懷", "現況說明", "方案金額", "是否成交", "洽談狀態", "未成交原因", "補充原因"]
            # Step 1: Create a mask where parent (e.g., 'Group') is not null
            mask = df_all['專案編號'].notnull()
            # Step 2: Create a group id that increments when a parent appears
            df_all['專案編號ID'] = mask.cumsum()
            # Step 3: Forward fill within each identified group
            df_all[parent_fields] = df_all.groupby('專案編號ID')[parent_fields].ffill()
            # Step 4: Drop helper column if no longer needed
            df_all.drop(columns='專案編號ID', inplace=True)
            # Define the sub-record fields
            sub_fields = ["項目分類", "處置代號", "治療項目", "牙位", "數量", "協同醫師1", "協同醫師2", "分項金額"]
            # Convert all str fields
            str_fields = ["專案編號", "專案日期", "成交日", "執行日", "方案類別", "病歷號碼", "姓名", "性別", "醫師", "諮詢師", "協同諮詢師", "電話關懷", "現況說明", "是否成交", "洽談狀態", "未成交原因", "補充原因", "項目分類", "處置代號", "治療項目", "牙位", "協同醫師1", "協同醫師2"]
            int_fields = ["年齡", "方案金額", "數量", "分項金額"]
            df_all[str_fields] = df_all[str_fields].map(clean_numeric_to_str)
            df_all[int_fields] = df_all[int_fields].map(convert_numeric_to_int)
            # Group by parent_fields, then aggregate sub_records
            grouped = df_all.groupby(parent_fields)[sub_fields] \
                        .apply(lambda x: x.to_dict(orient='records')) \
                        .reset_index() \
                        .rename(columns={0: 'sub_records'})
            return grouped
        except Exception as e:
            err_msg = f"Error in transform: {e}"
            logger.error(err_msg, exc_info=True)
        finally:
            logger.info("Ending func transform")
