from typing import Any, Dict, Optional, List
import awswrangler as wr
from stdout_logger import Logger

logger = Logger("ReadDataCatalogUtil").get_logger()


def lambda_handler(event: Dict[str, List[Dict[str, Any]]], context: Optional[Any]) -> None:
    """
    Handles the AWS Lambda event for reading data from multiple AWS Glue Catalog tables into Pandas DataFrames.

    :param event: A dictionary containing a list of configurations, each with a database and table name to read.
    :param context: AWS Lambda context object, not used in this function but included for AWS Lambda compatibility.
    :rtype: None
    """
    configurations = event.get('configurations', [])

    for config in configurations:
        database = config.get('database', 'default')
        table_name = config.get('table_name', '')

        if not table_name:
            logger.warning("Table name is missing in one of the configurations.")
            continue

        try:
            logger.info(f"Reading table '{table_name}' from database '{database}'.")
            df = wr.athena.read_sql_table(table=table_name, database=database)

            logger.info(len(df)) # 28, 374, 2003
            # logger.info(df)

            logger.info(f"Table '{table_name}' from database '{database}' read successfully.")
        except Exception as e:
            logger.error(f"Error reading table '{table_name}' from database '{database}': {str(e)}")


# Ejemplo de evento de entrada
event = {
    "configurations": [
        {
            "database": "default",
            "table_name": "departments"
        },
        {
            "database": "default",
            "table_name": "jobs"
        },
        {
            "database": "default",
            "table_name": "employees"
        }
    ]
}

# Descomenta la siguiente línea para probar localmente. Comenta o elimina antes de desplegar en AWS Lambda.
lambda_handler(event=event, context=None)