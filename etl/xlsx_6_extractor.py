from pandas import DataFrame
import pandas as pd
from datetime import date
import logging
import re

from xlsx_interface_extractor import IXlsxExtractor
from utils.utils import convert_minguo_date, clean_numeric_to_str, convert_numeric_to_int

logger = logging.getLogger(__name__)

class Xlsx6Extractor(IXlsxExtractor):
    TABLE_NAME = "治療專案執行明細表"
    # File name: 6.治療專案執行明細表/治療專案執行明細表_1140401_1140401_不分醫師.xls
    FILE_PATTERN = r"6.治療專案執行明細表/治療專案執行明細表_(\d{7})_(\d{7})_不分醫師\.xls$"
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
            logger.info("scan all sheets:%s", ", ".join(xls.sheet_names))
            # Read and concatenate all sheets
            df_all = pd.concat(
                [xls.parse(sheet_name, usecols="A:S") for sheet_name in xls.sheet_names],
                ignore_index=True
            )
            # ["病歷號","姓名","醫師","專案日期","成交日期","專案編號","專案類別","執行日期","處置代號","執行項目","牙位","金額","付款方式","專案金額","已收金額","未收金額","付款日期","付款金額","協同醫師"]
            # Define the parent-record fields
            parent_fields = ["病歷號","姓名","醫師","專案日期","成交日期","專案編號","專案類別","專案金額","已收金額","未收金額"]
            df_all["金額"] = df_all["金額"].replace(0, None)
            df_all[parent_fields] = df_all[parent_fields].ffill()
            # Convert all str fields
            str_fields = ["病歷號","姓名","醫師","專案日期","成交日期","專案編號","專案類別","執行日期","處置代號","執行項目","牙位","付款方式","付款日期","協同醫師"]
            df_all[str_fields] = df_all[str_fields].map(clean_numeric_to_str)
            int_fields = ["金額","專案金額","已收金額","未收金額","付款金額"]
            df_all[int_fields] = df_all[int_fields].map(convert_numeric_to_int)
            df_all["處置代號"] = df_all["處置代號"].str.strip()
            # Combine fields into a dictionary
            df_all['operation_records'] = df_all.apply(lambda row: {'執行日期':row['執行日期'],'處置代號':row['處置代號'],'執行項目':row['執行項目'],'牙位':row['牙位'],'金額':row['金額'],'付款方式':row['付款方式']}, axis=1)
            df_all['payment_records'] = df_all.apply(lambda row: {'付款日期':row['付款日期'],'付款金額': row['付款金額'],'協同醫師':row['協同醫師']}, axis=1)
            # Group by parent_fields, then aggregate sub_records
            grouped = df_all.groupby(parent_fields).agg({
                'operation_records': lambda x: [r for r in list(x) if r['執行日期']],
                'payment_records': lambda x: [r for r in list(x) if r['付款金額']]
            }).reset_index()
            return grouped
        except Exception as e:
            err_msg = f"Error in transform: {e}"
            logger.error(err_msg, exc_info=True)
        finally:
            logger.info("Ending func transform")
