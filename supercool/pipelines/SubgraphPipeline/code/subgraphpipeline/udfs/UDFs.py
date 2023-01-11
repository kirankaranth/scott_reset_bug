from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.lookups import (
    createLookup,
    createRangeLookup,
    lookup,
    lookup_last,
    lookup_match,
    lookup_count,
    lookup_row,
    lookup_row_reverse,
    lookup_nth
)

def registerUDFs(spark: SparkSession):
    spark.udf.register("squaresies", squaresies)

@udf(returnType = IntegerType())
def squaresies(value: int):
    return value * value
