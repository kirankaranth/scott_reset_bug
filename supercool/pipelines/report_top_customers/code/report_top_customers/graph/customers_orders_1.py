from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from report_top_customers.config.ConfigStore import *
from report_top_customers.udfs.UDFs import *

def customers_orders_1(spark: SparkSession) -> DataFrame:
    return spark.sql(f"SELECT * FROM scottdemo.customers_orders WHERE customer_id == 1")
