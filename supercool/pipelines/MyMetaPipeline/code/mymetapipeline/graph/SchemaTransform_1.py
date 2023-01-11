from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from mymetapipeline.config.ConfigStore import *
from mymetapipeline.udfs.UDFs import *

def SchemaTransform_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0
