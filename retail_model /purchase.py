from pyspark.sql.types import *


class Purchase:

    schema = StructType(
        [
            StructField("purchaseid", StringType(), False),
            StructField("customerid", StringType(), False),
            StructField("productid", StringType(), False),
            StructField("quantity", IntegerType(), False),
            StructField("priceperunit", DoubleType(), False),
            StructField("purchasetimestamp", TimestampType(), False),
        ]
    )
