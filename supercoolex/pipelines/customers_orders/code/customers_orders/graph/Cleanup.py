from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from customers_orders.config.ConfigStore import *
from customers_orders.udfs.UDFs import *

def Cleanup(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        datediff(current_date(), col("account_open_date")).alias("account_length_days"), 
        col("order_id"), 
        col("customer_id"), 
        col("amount")
    )
