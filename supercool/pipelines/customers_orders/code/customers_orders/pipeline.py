from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from customers_orders.config.ConfigStore import *
from customers_orders.udfs.UDFs import *
from prophecy.utils import *
from customers_orders.graph import *

def pipeline(spark: SparkSession) -> None:
    df_orders = orders(spark)
    df_orders = collectMetrics(
        spark, 
        df_orders, 
        "graph", 
        "rPAIuwDmq8wRor7DyKZax$$8A1ILhO7hFjsMomKihDL1", 
        "rrE9EIn6fZIP3JjAzuiNX$$Gm330ZAC-57_gZ-jCVb8c"
    )
    df_customers = customers(spark)
    df_customers = collectMetrics(
        spark, 
        df_customers, 
        "graph", 
        "uNtJnICwfKv_ooKP9IjsN$$NRqYPzWKCGeHPEySd-7Sq", 
        "OnTMNqxbigMCD3M14OhWr$$J7SQ7cPUHbF5Ek7eOKz2w"
    )
    df_By_CustomerId = By_CustomerId(spark, df_orders, df_customers)
    df_By_CustomerId = collectMetrics(
        spark, 
        df_By_CustomerId, 
        "graph", 
        "93DFXOMtuH2f5Xq4_Cdqi$$rh2cfpyFIjbj53qu0U5IZ", 
        "l5_p0m9fgJ32uNjyozV2h$$jA94aU8AnriJNBCAcOjEM"
    )
    df_Account_Length_Subgraph = Account_Length_Subgraph(spark, df_By_CustomerId)
    df_SumAmounts = SumAmounts(spark, df_Account_Length_Subgraph)
    df_SumAmounts = collectMetrics(
        spark, 
        df_SumAmounts, 
        "graph", 
        "CJrTkjxKezt0GaymeWYQ4$$rOqyJaqbqDFgtklKNC1v9", 
        "njxDmpL-AZxfQkJfbmjf6$$ioHwtNMH0FzuI2PDne7IR"
    )
    Customers_Orders(spark, df_SumAmounts)
    Script_1(spark, df_SumAmounts)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    MetricsCollector.initializeMetrics(spark)
    spark.conf.set("prophecy.collect.basic.stats", "true")
    spark.conf.set("spark.sql.legacy.allowUntypedScalaUDF", "true")
    spark.conf.set("spark.sql.optimizer.excludedRules", "org.apache.spark.sql.catalyst.optimizer.ColumnPruning")
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/customers_orders")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/customers_orders")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
