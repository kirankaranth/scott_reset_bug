from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from report_top_customers.config.ConfigStore import *
from report_top_customers.udfs.UDFs import *

def ByTotalAmount(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.orderBy(col("amounts").desc(), col("customer_is_cool").asc())
