import logging
import pandas as pd
from pandas import DataFrame
from google.cloud import bigquery

logger = logging.getLogger(__name__)


class BigQueryLoader:
    def load_to_bq_by_partition_date(self,
                                    df: DataFrame,
                                    full_table_id: str,
                                    partition_date: str):  # ex: 20250301
        logger.info(f"Starting func load_to_bq to {full_table_id})")

        try:
            partition = "partition_date"
            client = bigquery.Client()
            job_config = bigquery.LoadJobConfig()
            job_config.create_disposition = bigquery.CreateDisposition.CREATE_IF_NEEDED
            job_config.ignore_unknown_values = True
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
            job_config.autodetect = True
            job_config.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field=partition
            )
            full_table_id += f"${partition_date}"
            df[partition] = pd.to_datetime(partition_date, format='%Y%m%d')
            df[partition] = df[partition].dt.date
            job = client.load_table_from_dataframe(df, full_table_id, job_config=job_config)
            logger.info(f"---- starting bigquery loadjob: {job.job_id}")
            result = job.result()
            logger.info(
                f"---- successful bigquery loadjob: {result.job_id} "
                f"on {full_table_id} with {result.output_rows} records"
            )
        except Exception as e:
            logger.error(f"Error in load_to_bq: {e}", exc_info=True)
        finally:
            logger.info("Ending func load_to_bq")
