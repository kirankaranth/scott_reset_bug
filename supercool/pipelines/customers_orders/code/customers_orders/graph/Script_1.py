from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from customers_orders.config.ConfigStore import *
from customers_orders.udfs.UDFs import *

def Script_1(spark: SparkSession, in0: DataFrame):
    in0.createOrReplaceTempView("tmpview")
    spark.sql("CREATE VIEW `scottdemo`.`testview2` AS SELECT * FROM tmpview")

    return 
