from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from . import *

def Account_Length_Subgraph(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df_SchemaTransform_1 = SchemaTransform_1(spark, in0)
    df_SchemaTransform_1 = collectMetrics(
        spark, 
        df_SchemaTransform_1, 
        "Account_Length_Subgraph", 
        "TZcrrkZr_m3cEUPqK22vx$$EYFHOfdOcpywpuQB6AL-0", 
        "xB2doqoBekW-qnLk6owRm$$1pSn317eNu2HB57nAZIyH"
    )

    return df_SchemaTransform_1
