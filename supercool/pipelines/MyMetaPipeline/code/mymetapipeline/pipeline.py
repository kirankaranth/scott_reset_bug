from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from mymetapipeline.config.ConfigStore import *
from mymetapipeline.udfs.UDFs import *
from prophecy.utils import *
from mymetapipeline.graph import *

def pipeline(spark: SparkSession) -> None:
    df_component_runs = component_runs(spark)
    df_Filter_1_1 = Filter_1_1(spark, df_component_runs)
    df_SchemaTransform_2 = SchemaTransform_2(spark, df_Filter_1_1)
    df_pipeline_runs = pipeline_runs(spark)
    df_Filter_1 = Filter_1(spark, df_pipeline_runs)
    df_SchemaTransform_1 = SchemaTransform_1(spark, df_Filter_1)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/MyMetaPipeline")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/MyMetaPipeline")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
