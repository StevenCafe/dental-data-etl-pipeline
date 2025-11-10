import json
from pandas import DataFrame
import pandas as pd
from datetime import date
import logging
import re

from xlsx_interface_extractor import IXlsxExtractor
from utils.utils import convert_minguo_date, clean_numeric_to_str

logger = logging.getLogger(__name__)

class Xlsx7Extractor(IXlsxExtractor):
    TABLE_NAME = "電子處置單"
    # File name: 7.電子處置單/電子處置單_1140422_1140422.xls
    FILE_PATTERN = r"7.電子處置單/電子處置單_(\d{7})_(\d{7})\.xls$"
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
            all_sheets = [sheet for sheet in xls.sheet_names if "電子處置單" in sheet]
            logger.info("scan all sheets:%s", ", ".join(all_sheets))
            # Read and concatenate all sheets
            df_all = pd.concat(
                [xls.parse(sheet_name, usecols="A:F") for sheet_name in all_sheets],
                ignore_index=True
            )
            # ["病歷號碼","姓名","身份證字號","手機","門診日","處置單內容"]
            # Parse JSON strings into dicts
            dicts = df_all["處置單內容"].apply(json.loads)
            # Normalize into a flat DataFrame
            expanded = pd.json_normalize(dicts)
            # Combine with original DataFrame (dropping the raw JSON column)
            df_all = pd.concat([df_all.drop(columns="處置單內容"), expanded], axis=1)
            # Define the expanded fields
            # {"健保":"0,0,0,0,","掛號費":1,"處方":0,"預約備註":"約5月在華盛頓的時間   口掃非當天   60分鐘 /汶","間隔":null,"間隔天數":"","需時":1,"需時_分":"","定檢":null,"下次費用":"","備註":"","治療項目":"","收費":-1,"收費金額":"","衛教單":"","內轉科別":null,"內轉醫師代號":"","內轉說明":""}
            # ["健保","掛號費","處方","預約備註","間隔","間隔天數","需時","需時_分","定檢","下次費用","備註","治療項目","收費","收費金額","衛教單","內轉科別","內轉醫師代號","內轉說明"]
            # Convert all str fields
            str_fields = ["病歷號碼","姓名","身份證字號","手機","門診日","健保","掛號費","處方","預約備註","定檢","備註","治療項目","收費","衛教單","內轉科別","內轉醫師代號","內轉說明"]
            df_all[str_fields] = df_all[str_fields].map(clean_numeric_to_str)
            int_fields = ["間隔","間隔天數","需時","需時_分","下次費用", "收費金額"]
            df_all[int_fields] = df_all[int_fields].apply(pd.to_numeric, errors='coerce')
            df_all[int_fields] = df_all[int_fields].fillna(0).astype(int)
            return df_all
        except Exception as e:
            err_msg = f"Error in transform: {e}"
            logger.error(err_msg, exc_info=True)
        finally:
            logger.info("Ending func transform")
