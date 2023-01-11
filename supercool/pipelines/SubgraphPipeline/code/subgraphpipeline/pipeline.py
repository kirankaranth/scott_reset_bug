from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from subgraphpipeline.config.ConfigStore import *
from subgraphpipeline.udfs.UDFs import *
from prophecy.utils import *
from subgraphpipeline.graph import *

def pipeline(spark: SparkSession) -> None:
    df_customers_0 = customers_0(spark)
    df_TopX_Subgraph_1 = TopX_Subgraph_1(spark, df_customers_0)
    df_SchemaTransform_3 = SchemaTransform_3(spark, df_customers_0)
    df_SchemaTransform_1 = SchemaTransform_1(spark, df_TopX_Subgraph_1)
    df_Cleanup_Subgraph_1 = Cleanup_Subgraph_1(spark, df_SchemaTransform_3)
    df_SchemaTransform_2 = SchemaTransform_2(spark, df_Cleanup_Subgraph_1)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SubgraphPipeline")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/SubgraphPipeline")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
