from datetime import datetime
import json

from pydantic import BaseModel, ValidationError, Field, field_validator
from typing import List, Optional, Type, Dict, Any
import pymysql
from utils.aws_secrets_manager_connector import get_secret
from utils.stdout_logger import Logger

logger = Logger("InsertDataToRDS").get_logger()


class Department(BaseModel):
    """
    Represents a department entity with optional ID and required department name.
    """

    id: int
    department_name: str


class Job(BaseModel):
    """
    Represents a job entity with optional ID and required job name.
    """

    id: int
    job_name: str


class Employee(BaseModel):
    """
    Represents an employee entity with optional ID, required name, ISO 8601 formatted datetime,
    department ID, and job ID.
    """

    id: int
    name: str
    datetime: str = Field(...)
    department_id: int
    job_id: int

    @field_validator("datetime")
    def datetime_format(cls, v):
        """
        Validates that the datetime is in ISO 8601 format.

        :param v: The datetime string to validate.
        :type v: str
        :return: The validated datetime string.
        :rtype: str
        :raises ValueError: If the datetime is not in ISO 8601 format.
        """
        try:
            datetime.strptime(v, "%Y-%m-%dT%H:%M:%SZ")
        except ValueError:
            raise ValueError("datetime must be in ISO 8601 format")
        return v


class Transaction(BaseModel):
    """
    Represents a transaction containing lists of departments, jobs, and employees.
    """

    departments: List[Department] = []
    jobs: List[Job] = []
    employees: List[Employee] = []


def insert_data(conn, table: str, data: List[dict]) -> None:
    """
    Inserts a list of records into the specified table in the database.

    :param conn: The database connection object.
    :param table: The name of the table to insert data into.
    :param data: A list of dictionaries, each representing a record to insert.
    :type table: str
    :type data: List[dict]
    :rtype: None
    """
    with conn.cursor() as cursor:
        for record in data:
            columns = ", ".join(record.keys())
            placeholders = ", ".join(["%s"] * len(record))
            values = tuple(record.values())

            query = f"INSERT INTO chall_g.{table} ({columns}) VALUES ({placeholders})"

            try:
                cursor.execute(query, values)
                logger.info(f"Inserted data into {table}: {record}")
            except pymysql.MySQLError as e:
                # TODO Insert errors in a dynamo table
                logger.error(f"Failed to insert data into {table}: {e}")

    conn.commit()


def process_records(
    conn: pymysql.connections.Connection,
    model: Type[BaseModel],
    data: List[dict],
    table_name: str,
) -> None:
    """
    Processes and inserts a list of records into a specified table.

    This function iterates over each record in the data list, validates it against a Pydantic model,
    and inserts the validated record into the specified table. If validation fails for a record,
    an error is logged, and the function continues to the next record.

    :param conn: The database connection object.
    :param model: The Pydantic model class used for validating each record.
    :param data: A list of dictionaries, each representing a record to validate and insert.
    :param table_name: The name of the table where records will be inserted.
    :type model: Type[BaseModel]
    :type data: List[dict]
    :type table_name: str
    :rtype: None
    """
    for record in data:
        try:
            validated_record = model(**record)
            insert_data(conn, table_name, [validated_record.dict(exclude_none=True)])
            logger.info(f"Record inserted into {table_name}: {validated_record}")
        except ValidationError as e:
            logger.error(f"Validation failed for record in {table_name}: {e}")


def lambda_handler(event: Dict[str, Any], context: Optional[Any]) -> Dict:
    """
        AWS Lambda function handler for processing and inserting data into an RDS database based on provided event data.

        This function establishes a database connection and then processes records for departments, jobs, and employees
        present in the event data. Each record is validated against a Pydantic model before insertion. If any part of the
        process fails, an error is logged.

        :param event: The event containing data to process. It must include a `body` key with serialized JSON data
                      representing the records to be processed.
        :param context: The context object (unused). This parameter is present to comply with AWS Lambda's handler
                        signature but is not used within the function.
        :type event: Dict[str, Any]
        :type context: Optional[Any]
        :return: A dictionary with keys 'statusCode', 'body', and 'headers', indicating the outcome of the database
                 operation. 'statusCode' is set to 200 for success, along with a success message in 'body'. In case of
                 failure, 'error' and 'message' keys in the return dictionary provide details.
        :rtype: Dict

        Usage:
            To use this function, invoke it with an `event` dictionary containing the data to process and an optional
            `context`. The function processes the data, validates it against specified models, and inserts it into an RDS
            database. The function returns a dictionary with a status code and body indicating the outcome of the operation.

        Example:
            example_event = {
                "body": {
                    "departments": [
                        {"id": 999, "department_name": "HR"},
                        {"id": 1000, "department_name": "Maintenance"}
                    ],
                    "jobs": [
                        {"id": 999, "job_name": "Engineer"},
                        {"id": 1000, "job_name": "Analyst"}
                    ],
                    "employees": [
                        {
                            "id": 1,
                            "name": "John Doe",
                            "datetime": "2021-11-07T02:48:42Z",
                            "department_id": 1,
                            "job_id": 1
                        },
                        {
                            "id": 2,
                            "name": "Jane Doe",
                            "datetime": "2021-12-07T02:48:42Z",
                            "department_id": 2,
                            "job_id": 2
                        }
                    ]
                },
                "isBase64Encoded": False
            }
            lambda_handler(example_event, None)
    """
    event["body"] = json.loads(event["body"])

    logger.info(f"InputEvent: {json.dumps(event, indent=4)}")

    secret = get_secret("chall_g_aurora_serverless")

    conn = None

    try:
        conn = pymysql.connect(
            host=secret["host"],
            user=secret["username"],
            passwd=secret["password"],
            db=secret["dbname"],
            connect_timeout=5,
        )
        logger.info("Database connection established.")

        for key in event["body"].keys():
            match key:
                case "departments":
                    process_records(conn, Department, event["body"][key], key)
                case "jobs":
                    process_records(conn, Job, event["body"][key], key)
                case "employees":
                    process_records(conn, Employee, event["body"][key], key)

    except Exception as e:
        logger.error({"error": "Database operation failed", "message": str(e)})
        return {"error": "Database operation failed", "message": str(e)}

    finally:
        if conn:
            conn.close()
            logger.info("Database connection closed.")

    return {
        "statusCode": 200,
        "body": json.dumps({"message": "Operation successful"}),
        "headers": {
            "Content-Type": "application/json"
        }
    }

# example_event = {
#     "body": {
#         "departments": [
#             {
#                 "id": 999,
#                 "department_name": 1
#             },
#             {
#                 "id": 1000,
#                 "department_name": "Maintenance"
#             }
#         ],
#         "jobs": [
#             {
#                 "id": 999,
#                 "job_name": "Engineer"
#             },
#             {
#                 "id": 1000,
#                 "job_name": "Analyst"
#             }
#         ],
#         "employees": [
#             {
#                 "id": 1,
#                 "name": "John Doe",
#                 "datetime": "2021-11-07T02:48:42Z",
#                 "department_id": 1,
#                 "job_id": 1
#             },
#             {
#                 "id": 2,
#                 "name": "Jane Doe",
#                 "datetime": "2021-12-07T02:48:42Z",
#                 "department_id": 2,
#                 "job_id": 2
#             }
#         ]
#     },
#     "isBase64Encoded": False
# }
#
#
# lambda_handler(example_event, None)
