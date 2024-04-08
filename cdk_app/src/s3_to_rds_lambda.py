from typing import Any, Dict, Optional

import awswrangler as wr

from utils.stdout_logger import Logger

logger = Logger("S3ToRdsLambda").get_logger()


def lambda_handler(event: Dict[str, Any], context: Optional[Any]) -> None:
    """
    Handles the AWS Lambda event for transferring data from S3 to an RDS MySQL instance.

    This function connects to an RDS MySQL instance using credentials stored in AWS Secrets Manager.
    It iterates through a list of table configurations provided in the event object, reads each specified
    Excel file from S3, and writes the data to the corresponding table in the MySQL database according
    to the mode specified ('overwrite' or 'append').

    :param event: A dictionary containing the AWS Secrets Manager secret_id and a list of table configurations.
                  Each table configuration should specify the S3 path to an Excel file, the target table name,
                  the database schema, column headers, and the mode ('overwrite' or 'append') for data insertion.
    :param context: AWS Lambda context object, not used in this function but included for AWS Lambda compatibility.
    :type event: Dict[str, Any]
    :type context: Optional[Any]
    :rtype: None
    :raises KeyError: If required keys in the `event` dictionary are missing.

    Usage example:
    ```
    event_config = {
        "secret_id": "your_aurora_serverless_secret_id",
        "tables": [
            {
                "s3_path": "s3://your-bucket/path/to/excel.xlsx",
                "table_name": "your_table",
                "schema": "your_schema",
                "headers": ["column1", "column2"],
                "mode": "overwrite"
            }
        ]
    }
    lambda_handler(event=event_config, context=None)
    ```
    """
    logger.info("Connecting to MySQL database...")
    con_mysql = wr.mysql.connect(secret_id=event['secret_id'])
    logger.info("Connection established.")

    for table_config in event['tables']:
        s3_path = table_config['s3_path']
        table_name = table_config['table_name']
        schema = table_config['schema']
        headers = table_config['headers']
        mode = table_config['mode']

        logger.info(f"Reading data from S3 path: {s3_path} for table: {table_name}")

        df = wr.s3.read_excel(s3_path, engine="openpyxl", header=None, names=headers)

        logger.info(f"Writing data to table: {schema}.{table_name} in mode: {mode}")

        wr.mysql.to_sql(df, con_mysql, schema=schema, table=table_name, mode=mode, index=False)

        logger.info(f"Data written successfully to table: {schema}.{table_name}")


event_config = {
    "secret_id": "chall_g_aurora_serverless",
    "tables": [
        {
            "s3_path": "s3://challenge-g-data-lake/raw/departments.xlsx",
            "table_name": "departments",
            "schema": "chall_g",
            "headers": ["id", "department_name"],
            "mode": "overwrite"
        },
        {
            "s3_path": "s3://challenge-g-data-lake/raw/hired_employees.xlsx",
            "table_name": "employees",
            "schema": "chall_g",
            "headers": ["id", "name", "datetime", "department_id", "job_id"],
            "mode": "overwrite"
        },
        {
            "s3_path": "s3://challenge-g-data-lake/raw/jobs.xlsx",
            "table_name": "jobs",
            "schema": "chall_g",
            "headers": ["id", "job_name"],
            "mode": "overwrite"
        }
    ]
}

lambda_handler(event=event_config, context=None)
