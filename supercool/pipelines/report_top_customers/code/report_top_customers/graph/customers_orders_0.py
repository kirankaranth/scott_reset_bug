from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from report_top_customers.config.ConfigStore import *
from report_top_customers.udfs.UDFs import *

def customers_orders_0(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"scottdemo.customers_orders")
