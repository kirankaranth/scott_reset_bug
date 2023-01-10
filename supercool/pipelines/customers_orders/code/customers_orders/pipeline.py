from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from customers_orders.config.ConfigStore import *
from customers_orders.udfs.UDFs import *
from prophecy.utils import *
from customers_orders.graph import *

def pipeline(spark: SparkSession) -> None:
    df_orders = orders(spark)
    df_customers = customers(spark)
    df_By_CustomerId = By_CustomerId(spark, df_orders, df_customers)
    df_Cleanup = Cleanup(spark, df_By_CustomerId)
    df_SumAmounts = SumAmounts(spark, df_Cleanup)
    Customers_Orders(spark, df_SumAmounts)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/customers_orders")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/customers_orders")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
