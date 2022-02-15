import configparser
import unittest
import logging
from pandas.testing import assert_frame_equal
from pyspark.sql import SparkSession


class PySparkTest(unittest.TestCase):

    cfg_parser = configparser.ConfigParser()
    spark = SparkSession.builder.master('local').appName('testing').getOrCreate()
    filepath = "C:\\Users\\B00827\\IdeaProjects\\POC2\\config.cfg"
    cfg_parser.read(filepath)
    sch = cfg_parser.get('schema', 'custom_schema')

    @classmethod
    def create_testing_pyspark_session(cls):
        return (SparkSession.builder
                .master('local')
                .appName('testing')
                .getOrCreate())

    @classmethod
    def logger_initializer(cls):
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)

    @classmethod
    def setUpClass(cls):
        cls.logger_initializer()
        cls.spark = cls.create_testing_pyspark_session()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    @classmethod
    def dataframe_equal(cls, df1, df2, columns):
        df1 = df1.toPandas()
        df1 = df1.sort_values(by=columns).reset_index(drop=True)
        df2 = df2.toPandas()
        df2 = df2.sort_values(by=columns).reset_index(drop=True)
        assert_frame_equal(df1, df2)