import os

from accounts import Account
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

from tests.parent import PySparkTest


class AccountTest(PySparkTest):
    """
    Class for testing Utility class and its methods
    """

    @classmethod
    def setUpClass(cls):
        cls.filename = "dataset/testAccountdata.csv"
        cls.schema = super().sch
        cls.a1 = Account(cls.filename, cls.schema)

    def test_add_group(self):
        pass

    def test_alert_key(self):
        pass

    def test_top_3_features(self):
        pass
