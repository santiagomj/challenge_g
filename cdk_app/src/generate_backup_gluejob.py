import sys
from typing import Dict

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

# Ensure the JOB_NAME argument is present for getResolvedOptions
if '--JOB_NAME' not in sys.argv:
    sys.argv += ['--JOB_NAME', 'RDSToS3']


def sparkSqlQuery(glueContext: GlueContext, query: str, mapping: Dict[str, DynamicFrame], transformation_ctx: str) -> DynamicFrame:
    """
    Executes a SQL query using Spark SQL on a set of DynamicFrames.

    This function registers each DynamicFrame in the mapping as a temporary SQL view,
    then executes the provided SQL query to produce a result set. The result set is
    converted back into a DynamicFrame.

    :param glueContext: The GlueContext object.
    :param query: The SQL query to execute.
    :param mapping: A dictionary mapping alias names to DynamicFrames. Each DynamicFrame will be registered as a temporary view with the alias as its name.
    :param transformation_ctx: A unique string to track this transformation context.
    :return: A DynamicFrame containing the result of the SQL query.
    """
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

configuraciones = [
    {"nombre_tabla": "departments", "ruta_s3": "s3://challenge-g-data-lake/backups/departments/", "transformation_ctx": "SQLQuery_departments", "catalog_table_name": "departments"},
    {"nombre_tabla": "employees", "ruta_s3": "s3://challenge-g-data-lake/backups/employees/", "transformation_ctx": "SQLQuery_employees", "catalog_table_name": "employees"},
    {"nombre_tabla": "jobs", "ruta_s3": "s3://challenge-g-data-lake/backups/jobs/", "transformation_ctx": "SQLQuery_jobs", "catalog_table_name": "jobs"}
]

for config in configuraciones:
    dynamic_frame = glueContext.create_dynamic_frame.from_options(
        connection_type="mysql",
        database = "chall_g",
        connection_options={
            "useConnectionProperties": "true",
            "dbtable": config["nombre_tabla"],
            "connectionName": "Mysql connection",
        },
        transformation_ctx=f"MySQL_{config['nombre_tabla']}",
    )
    
    SqlQuery = f"select * from {config['nombre_tabla']}"
    SQLQuery_dynamic_frame = sparkSqlQuery(
        glueContext,
        query=SqlQuery,
        mapping={"myDataSource": dynamic_frame},
        transformation_ctx=config["transformation_ctx"],
    )
    
    sink = glueContext.getSink(
        path=config["ruta_s3"],
        connection_type="s3",
        updateBehavior="UPDATE_IN_DATABASE",
        partitionKeys=[],
        enableUpdateCatalog=True,
        transformation_ctx=f"AmazonS3_{config['nombre_tabla']}",
    )
    sink.setCatalogInfo(catalogDatabase="default", catalogTableName=config["catalog_table_name"])
    sink.setFormat("avro")
    sink.writeFrame(SQLQuery_dynamic_frame)

job.commit()