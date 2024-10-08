import datetime
import shutil
import unittest
import warnings

from pyspark.sql import SparkSession
from pyspark.testing.utils import assertDataFrameEqual
from retail_model.purchase import Purchase
from streaming.purchase_analytics import PurchaseAnalytics
from pyspark.sql.types import *


class PurchaseAnalyticTableTest(unittest.TestCase):
    tmp_dir = f"/tmp/spark-streaming-test"
    table_path = f"{tmp_dir}/purchase_analytics_table_test"

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession. \
            builder. \
            appName('Purchase-Analytics-test'). \
            master('local[*]'). \
            getOrCreate()
        warnings.filterwarnings("ignore", category=ResourceWarning)

    @classmethod
    def tearDown(cls):
        cls.spark.stop()
        cls.remove_files(cls.table_path)

    def test_filter_purchases_test(self):
        lower_bound = 0.0
        upper_bound = 10.0
        records = [
            {"purchaseid": "id-1", "customerid": "c-1", "productid": "p-1", "quantity": 2, "priceperunit": 10.0, "purchasetimestamp": datetime.datetime.fromisoformat('2024-06-01T10:30:00')},
            {"purchaseid": "id-2", "customerid": "c-1", "productid": "p-2", "quantity": 3, "priceperunit": 1.0, "purchasetimestamp": datetime.datetime.fromisoformat('2024-06-01T11:30:00')}
        ]

        expected = [
            {"purchaseid": "id-2", "customerid": "c-1", "productid": "p-2", "quantity": 3, "priceperunit": 1.0, "purchasetimestamp": datetime.datetime.fromisoformat('2024-06-01T11:30:00')}
        ]

        df = self.spark.createDataFrame(records, Purchase.schema)

        df.write.mode("overwrite").format("parquet").save(self.table_path)

        streaming_df = self.spark.readStream. \
            format("parquet") \
            .schema(Purchase.schema) \
            .load(self.table_path)

        actual_streaming_df = PurchaseAnalytics.filter_purchases(streaming_df, lower_bound, upper_bound)

        query = actual_streaming_df.writeStream \
            .format("memory") \
            .queryName("filtered") \
            .outputMode("append") \
            .start()

# To Process all the input stream and stop the streaming once done
        query.processAllAvailable()

        actual_df = self.spark.sql("SELECT * FROM filtered")

        actual_df_dict = [row.asDict() for row in actual_df.collect()]
        self.assertEqual(len(expected), len(actual_df_dict))
        self.assertIn(expected[0], actual_df_dict)



    @staticmethod
    def remove_files(table_path):
        shutil.rmtree(table_path)
