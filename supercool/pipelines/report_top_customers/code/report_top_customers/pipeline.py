from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from report_top_customers.config.ConfigStore import *
from report_top_customers.udfs.UDFs import *
from prophecy.utils import *
from report_top_customers.graph import *

def pipeline(spark: SparkSession) -> None:
    df_customers_orders_0 = customers_orders_0(spark)
    df_customers_orders_0 = collectMetrics(
        spark, 
        df_customers_orders_0, 
        "graph", 
        "EI1kqQ02M7wcrJ7ucAc67$$dZf-9-6IDBvtUWAIU_OPE", 
        "Rhb0uuaSnVN_63WvmXvsH$$4fE_DWX_Vgw6gnZe_OBA9"
    )
    df_ByTotalAmount = ByTotalAmount(spark, df_customers_orders_0)
    df_ByTotalAmount = collectMetrics(
        spark, 
        df_ByTotalAmount, 
        "graph", 
        "fMVxortz3BGKVEB--u8VU$$9nns4Mv-GFMjg5cu41vJ5", 
        "n8Lt1SS_t2xg2F0hGDh14$$aqPkEXWP6nDoOI3akIhbC"
    )
    df_TopX_Subgraph = TopX_Subgraph(spark, df_ByTotalAmount)
    df_Reformat_1 = Reformat_1(spark, df_TopX_Subgraph)
    df_Reformat_1 = collectMetrics(
        spark, 
        df_Reformat_1, 
        "graph", 
        "bWelH5DOfPTRr680_IMkP$$6vhy7TmlFO9c7dd0jjHz6", 
        "dmNPdVjnTlTtMO8cDRZ0x$$1zY8fjurTTI6btdRWHv_q"
    )
    write_target(spark, df_Reformat_1)

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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/report_top_customers")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/report_top_customers")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
