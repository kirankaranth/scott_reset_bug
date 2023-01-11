from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from . import *

def TopX_Subgraph(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df_TopX_1 = TopX_1(spark, in0)
    df_TopX_1 = collectMetrics(
        spark, 
        df_TopX_1, 
        "TopX_Subgraph", 
        "ab3GRGoBynvwJH4I4T_3F$$M-QCcJFOhsc5S3v5E97_m", 
        "wWKSxV7JdJffeMT8rqp2Y$$V9RxhqkJjSEFLP1AgdivN"
    )

    return df_TopX_1
