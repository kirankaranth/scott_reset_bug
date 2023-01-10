from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from report_top_customers.config.ConfigStore import *
from report_top_customers.udfs.UDFs import *

def SQLStatement_1(spark: SparkSession, in0: DataFrame) -> (DataFrame):
    in0.createOrReplaceTempView("in0")
    df1 = spark.sql("SELECT * FROM in0\nORDER BY amounts DESC\nLIMIT 10")

    return df1
