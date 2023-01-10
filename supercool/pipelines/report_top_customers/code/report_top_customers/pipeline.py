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
    df_Script_1 = Script_1(spark, df_customers_orders_0)
    df_Script_1 = collectMetrics(
        spark, 
        df_Script_1, 
        "graph", 
        "TXJ-bK-XTgs8HF4td3Hfq$$IP8gx4iFz6GCKDjzHwIKf", 
        "oih-DeHlMdtPM-hCTDGmb$$dr48rpd-5ANKe7zgM8siG"
    )
    df_ByTotalAmount = ByTotalAmount(spark, df_customers_orders_0)
    df_ByTotalAmount = collectMetrics(
        spark, 
        df_ByTotalAmount, 
        "graph", 
        "fMVxortz3BGKVEB--u8VU$$9nns4Mv-GFMjg5cu41vJ5", 
        "n8Lt1SS_t2xg2F0hGDh14$$aqPkEXWP6nDoOI3akIhbC"
    )
    df_Top10 = Top10(spark, df_ByTotalAmount)
    df_Top10 = collectMetrics(
        spark, 
        df_Top10, 
        "graph", 
        "KtcK_QDcAsjlBBs53GY9Z$$CQIT4GVD1pUimk2ckV-Vx", 
        "XQk-NH3mYQayhQML12gS9$$kCrgutcwloaIB_W4IVXEp"
    )
    df_Reformat_1 = Reformat_1(spark, df_Top10)
    df_Reformat_1 = collectMetrics(
        spark, 
        df_Reformat_1, 
        "graph", 
        "bWelH5DOfPTRr680_IMkP$$6vhy7TmlFO9c7dd0jjHz6", 
        "dmNPdVjnTlTtMO8cDRZ0x$$1zY8fjurTTI6btdRWHv_q"
    )
    report_top_customers(spark, df_Reformat_1)
    df_SQLStatement_1 = SQLStatement_1(spark, df_customers_orders_0)
    df_SQLStatement_1 = collectMetrics(
        spark, 
        df_SQLStatement_1, 
        "graph", 
        "1XvAXgsXpp8BrdMV8Sw15$$xDFzhgtkK3oMs3x-zpC5t", 
        "Rmo7thOBTgFWVB6Ooj-Fs$$mrdBEwiCtLAvbT3xFuleX"
    )
    df_SchemaTransform_2 = SchemaTransform_2(spark, df_Script_1)
    df_SchemaTransform_2 = collectMetrics(
        spark, 
        df_SchemaTransform_2, 
        "graph", 
        "mp0na7taLaoA1DOyw1yZW$$Z8d5Qv6ySyQXhYRnNIoV7", 
        "UGMYxNK9_IxlK-AOHtQ9T$$AQ431VQj6rGv_q2pqSDLm"
    )
    df_SchemaTransform_2.cache().count()
    df_SchemaTransform_2.unpersist()
    df_SchemaTransform_1 = SchemaTransform_1(spark, df_SQLStatement_1)
    df_SchemaTransform_1 = collectMetrics(
        spark, 
        df_SchemaTransform_1, 
        "graph", 
        "VqOsGeJEr9nR63RWf6Rj5$$Uf0QXb8gSrOETADQn1GhA", 
        "xE99ksLvNljx_LsUWX4V1$$nO94Hs366zb0KqJG8uRSb"
    )
    df_SchemaTransform_1.cache().count()
    df_SchemaTransform_1.unpersist()

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
