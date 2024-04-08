import sys

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Ensure the JOB_NAME argument is present for getResolvedOptions
if '--JOB_NAME' not in sys.argv:
    sys.argv += ['--JOB_NAME', 'S3ToRDS']

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

configuraciones = [
    {"ruta_s3": "s3://challenge-g-data-lake/backups/departments/", "nombre_tabla": "departments"},
    {"ruta_s3": "s3://challenge-g-data-lake/backups/employees/", "nombre_tabla": "employees"},
    {"ruta_s3": "s3://challenge-g-data-lake/backups/jobs/", "nombre_tabla": "jobs"},
]

for config in configuraciones:
    s3_data = glueContext.create_dynamic_frame.from_options(
        format_options={},
        connection_type="s3",
        format="avro",
        connection_options={
            "paths": [config["ruta_s3"]],
            "recurse": True,
        },
        transformation_ctx="AmazonS3_node1712531281316",
    )

    glueContext.write_dynamic_frame.from_jdbc_conf(
        frame=s3_data,
        catalog_connection="Mysqlwriterconnection",
        connection_options={"dbtable": config["nombre_tabla"], "database": "chall_g"},
        transformation_ctx="write_to_mysql"
    )

job.commit()