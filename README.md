# Dental Data ETL Pipeline

## Overview
To automate the ingestion of daily Excel files containing dental clinic treatment records into the data warehouse.

## Work Flow
```plaintext
Data Source (GCS)
    ↓
Extract → Transform → Load (Cloud Function)
    ↓
Data Warehouse (Bigquery)
    ↓
Analytics Dashboard (Power BI)
```

## Project Structure
```plaintext
root/
├── config/
│   └── logging_config.py
├── utils/
│   └── utils.py
├── etl/
│   ├── bigquery_loader.py
│   ├── xlsx_1_extractor.py
│   ├── xlsx_2_extractor.py
│   ├── xlsx_3_extractor.py
│   ├── xlsx_4_extractor.py
│   ├── xlsx_5_extractor.py
│   ├── xlsx_6_extractor.py
│   ├── xlsx_7_extractor.py
│   └── xlsx_interface_extractor.py
└── main.py
```

## Source GCP Bucket (******d): 
1. 治療計劃明細表/治療計劃明細表_分醫師_1130401_1130401.xls

2. 全日預約表/全日總預約表_1130101.xls

3. Dr right reviews/[Dr.Right] reviews_***牙醫診所01_2025.04.01_2025.04.01_1743556500.0.xlsx

4. Dr right invitations/[Dr.Right] invitations_***牙醫診所00_2025.04.01_2025.04.01_1743556500.0.xlsx

5. 期間初複診明細表/期間初複診明細表_1140401_1140401.xls

6. 治療專案執行明細表/治療專案執行明細表_1140401_1140401_不分醫師.xls

7. 電子處置單/電子處置單_1140422_1140422.xls

## Destination Bigquery Dataset (raw_dental_treatments):
- 治療計劃明細表 (Partitioned by Day)
- 全日總預約表 (Partitioned by Day)
- reviews (Partitioned by Day)
- invitations (Partitioned by Day)
- 期間初複診明細表 (Partitioned by Day)
- 治療專案執行明細表 (Partitioned by Day)
- 電子處置單 (Partitioned by Day)

### P.S.
1. The file names will always follow the format below:

> - XXX_{start_date}.xls
> - XXX_{start_date}_{end_date}.xls

2. The partition_date will use {start_date} in the file name
