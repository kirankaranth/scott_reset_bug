from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from subgraphpipeline.config.ConfigStore import *
from subgraphpipeline.udfs.UDFs import *

def SchemaTransform_2(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0
