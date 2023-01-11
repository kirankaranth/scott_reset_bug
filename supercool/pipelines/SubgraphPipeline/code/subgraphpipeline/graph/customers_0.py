from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from subgraphpipeline.config.ConfigStore import *
from subgraphpipeline.udfs.UDFs import *

def customers_0(spark: SparkSession) -> DataFrame:
    return spark.read\
        .schema(
          StructType([
            StructField("customer_id", IntegerType(), True), StructField("first_name", StringType(), True), StructField("last_name", StringType(), True), StructField("phone", StringType(), True), StructField("email", StringType(), True), StructField("country_code", StringType(), True), StructField("account_open_date", DateType(), True), StructField("account_flags", StringType(), True)
        ])
        )\
        .option("header", True)\
        .option("sep", ",")\
        .csv("dbfs:/Prophecy/scott+demo3@prophecy.io/CustomersDatasetInput.csv")
