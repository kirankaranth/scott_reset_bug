from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from report_top_customers.config.ConfigStore import *
from report_top_customers.udfs.UDFs import *
from prophecy.utils import *
from report_top_customers.graph import *

def pipeline(spark: SparkSession) -> None:
    df_customers_orders_0 = customers_orders_0(spark)
    df_Script_1 = Script_1(spark, df_customers_orders_0)
    df_ByTotalAmount = ByTotalAmount(spark, df_customers_orders_0)
    df_Top10 = Top10(spark, df_ByTotalAmount)
    df_Reformat_1 = Reformat_1(spark, df_Top10)
    report_top_customers(spark, df_Reformat_1)
    df_SQLStatement_1 = SQLStatement_1(spark, df_customers_orders_0)
    df_SchemaTransform_2 = SchemaTransform_2(spark, df_Script_1)
    df_SchemaTransform_1 = SchemaTransform_1(spark, df_SQLStatement_1)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/report_top_customers")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/report_top_customers")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
