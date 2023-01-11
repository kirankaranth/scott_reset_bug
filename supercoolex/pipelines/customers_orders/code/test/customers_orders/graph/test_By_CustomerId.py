from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from argparse import Namespace
from prophecy.test import BaseTestCase
from prophecy.test.utils import *
from customers_orders.graph.By_CustomerId import *
import customers_orders.config.ConfigStore as ConfigStore


class By_CustomerIdTest(BaseTestCase):

    def test_unit_test_0(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/customers_orders/graph/By_CustomerId/in0/schema.json',
            'test/resources/data/customers_orders/graph/By_CustomerId/in0/data/test_unit_test_0.json',
            'in0'
        )
        dfIn1 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/customers_orders/graph/By_CustomerId/in1/schema.json',
            'test/resources/data/customers_orders/graph/By_CustomerId/in1/data/test_unit_test_0.json',
            'in1'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/customers_orders/graph/By_CustomerId/out/schema.json',
            'test/resources/data/customers_orders/graph/By_CustomerId/out/data/test_unit_test_0.json',
            'out'
        )
        dfOutComputed = By_CustomerId(self.spark, dfIn0, dfIn1)
        assertDFEquals(
            dfOut.select("order_id", "customer_id", "account_open_date", "amount"),
            dfOutComputed.select("order_id", "customer_id", "account_open_date", "amount"),
            self.maxUnequalRowsToShow
        )

    def setUp(self):
        BaseTestCase.setUp(self)
        import os
        fabricName = os.environ['FABRIC_NAME']
        ConfigStore.Utils.initializeFromArgs(
            self.spark,
            Namespace(file = f"configs/resources/config/{fabricName}.json", config = None)
        )
