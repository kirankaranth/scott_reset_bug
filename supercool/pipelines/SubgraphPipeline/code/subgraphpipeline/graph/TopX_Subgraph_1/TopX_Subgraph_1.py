from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from . import *

def TopX_Subgraph_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df_TopX_1 = TopX_1(spark, in0)

    return df_TopX_1
