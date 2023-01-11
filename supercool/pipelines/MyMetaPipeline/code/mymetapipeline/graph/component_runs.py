from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from mymetapipeline.config.ConfigStore import *
from mymetapipeline.udfs.UDFs import *

def component_runs(spark: SparkSession) -> DataFrame:
    return spark.read.format("delta").load("dbfs:/prophecy/metadata/executionmetricsapp/component_runs/")
