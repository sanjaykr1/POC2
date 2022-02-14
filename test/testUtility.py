import os

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

from Utility import Utility
from test.parent import PySparkTest


class MyTestClass(PySparkTest):
    """
    Class for testing Utility class and its methods
    """

    @classmethod
    def setUpClass(cls):
        cls.u1 = Utility()

    def test_readfile(self):
        filename = "dataset/testAccountdata.csv"
        schema = super().sch
        df = self.u1.readfile(filename, schema)
        self.assertTrue(df.head())

    def test_utility_write_data(self):
        cols = ["col1", "col2", "col3"]
        data = [("1", "Val1", "Abc1"),
                ("2", "Val2", "Abc2"),
                ("3", "Val3", "Abc3")]
        test_df = super().spark.createDataFrame(data, cols)
        self.u1.writefile(test_df, "test_utility_write_data")
        self.assertTrue(os.path.isdir("C:\\Users\\ACER NITRO 5\\IdeaProjects\\POC2\\test\\test_utility_write_data"))

    def test_custom_schema(self):
        new_schema = StructType([
            StructField("Col1_StringType", StringType(), True),
            StructField("Col2_DoubleType", DoubleType(), True),
            StructField("Col3_TimestampType", TimestampType(), True),
        ])
        test_schema = "Col1_StringType StringType(),Col2_DoubleType DoubleType(),Col3_TimestampType TimestampType()"
        schema_result = self.u1.custom_schema(test_schema)
        self.assertEquals(new_schema, schema_result)
