from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from customers_orders.config.ConfigStore import *
from customers_orders.udfs.UDFs import *

def SchemaTransform_2(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0
