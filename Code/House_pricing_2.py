import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import *
from pyspark.sql.types import TimestampType,StringType,IntegerType, DateType
from pyspark.sql.functions import * #udf, lit, current_date, concat_ws
import pyspark.sql.functions as F
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from datetime import timedelta
from datetime import datetime
from datetime import date

f = date.today()
Fecha = str(f)

# Create the enviorement
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
path = [path1] # the path of the csv file in s3

# Script generated for node S3 bucket
DyF_S3 = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": '"', "withHeader": True, "separator": ","},
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": [path],
        "recurse": True,},
    transformation_ctx="S3bucket_node1",)

# Transform the dynamic frame to spark dataframe
df = DyF_S3.toDF()

# Eliminate duplicates
df = df.dropDuplicates()

# To eliminate '+' values we use regex
regex = '([0-9])+'

df = df.withColumn("Cuartos", regexp_extract('Cuartos', regex, 0).cast('integer'))
df = df.withColumn("Baños", regexp_extract('Baños', regex, 0).cast('integer'))
df = df.withColumn("Cocheras", regexp_extract('Cocheras', regex, 0).cast('integer'))
df = df.withColumn("Area_total", regexp_extract('Area_total', regex, 0).cast('integer'))

# Eliminate the ','
df = df.withColumn('Precio', F.regexp_replace(F.col('Precio'), ',',''))
df = df.withColumn("Precio", regexp_extract('Precio', regex, 0).cast('integer'))

DyFfinal = DynamicFrame.fromDF(df, glueContext, "DyFfinal")

# Set the destination for the parquet files
DataSink0 = glueContext.getSink(path = [path2],
                                connection_type = "s3",
                                updateBehavior = "UPDATE_IN_DATABASE",
                                partitionKeys = [],
                                compression = "snappy",
                                enableUpdateCatalog = True,
                                transformation_ctx = "DataSink0")
        
#
# Writing DynamicFrame
DataSink0.setCatalogInfo(catalogDatabase = [aws_database], catalogTableName = "real_state")
DataSink0.setFormat("glueparquet")
DataSink0.writeFrame(DyFfinal)

#Write with spark
df.write.mode("overwrite").format("parquet").save([path2])
 
job.commit()