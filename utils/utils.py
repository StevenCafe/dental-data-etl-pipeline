import pandas as pd
from datetime import datetime, date


def convert_minguo_date(date_str) -> date:
    """
    '1130701' will return as date 2024-07-01
    """
    year = int(date_str[:3]) + 1911
    return datetime.strptime(f"{year}{date_str[3:]}", "%Y%m%d").date()


def clean_numeric_to_str(val):
    if pd.isnull(val):
        return ""
    if isinstance(val, float) and val.is_integer():
        return str(int(val))  # removes .0
    return str(val)


def convert_numeric_to_int(val):
    if pd.isnull(val):
        return int(0)
    if isinstance(val, float) and val.is_integer():
        return int(val)  # removes .0
    return int(val)
