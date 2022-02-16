import os
from datetime import datetime

from accounts import Account
from pyspark.sql.types import *

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
        # data = {
        #     ("1", "Shivam", "Sanjay", "HighRisk", 5.0, "Deposit", 3.0, "Transfer", 3.0, "Credit", 2.0, "Cash", 1.0, 14.0,
        #      datetime.strptime('2017-01-04 16:12:56.884235', '%Y-%m-%d %H:%M:%S.%f'), "201701"),
        #     ("2", "Sanjay", "Prakash", "Transfer", 1.0, "Deposit", 5.0, "Credit", 4.0, "Cash", 5.0, "HighRisk", 3.0, 18.0,
        #      datetime.strptime("2016-11-01 17:01:00.948878", '%Y-%m-%d %H:%M:%S.%f'), "201611"),
        #     ("3", "Prakash", "Suman", "HighRisk", 2.0, "Deposit", 2.0, "Transfer", 1.0, "Credit", 4.0, "Cash", 4.0, 13.0,
        #      datetime.strptime("2016-12-31 06:11:43.456842", '%Y-%m-%d %H:%M:%S.%f'), "201612"),
        #     ("4", "Suman", "Shivam", "Cash", 4.0, "Transfer", 3.0, "HighRisk", 4.0, "Credit", 5.0, "Deposit", 2.0, 18.0,
        #      datetime.strptime("2017-02-14 11:23:22.299687", '%Y-%m-%d %H:%M:%S.%f'), "201702"),
        #     ("5", "Robert", "Edward", "Deposit", 3.0, "Cash", 4.0, "Transfer", 5.0, "Credit", 3.0, "HighRisk", 4.0, 19.0,
        #      datetime.strptime("2017-06-10 11:31:27.948881", '%Y-%m-%d %H:%M:%S.%f'), "201706"),
        #     ("6", "Daniel", "Robert", "Credit", 5.0, "Cash", 1.0, "Deposit", 5.0, "HighRisk", 3.0, "Transfer", 2.0, 16.0,
        #      datetime.strptime("2017-06-10 11:30:27.999999", '%Y-%m-%d %H:%M:%S.%f'), "201706"),
        #     ("7", "Alphonse", "Daniel", "HighRisk", 5.0, "Deposit", 3.0, "Transfer", 3.0, "Credit", 4.0, "Cash", 4.0, 19.0,
        #      datetime.strptime("2017-06-10 11:30:27.000000", '%Y-%m-%d %H:%M:%S.%f'), "201706"),
        #     ("8", "Edward", "Alphonse", "Credit", 3.0, "Deposit", 2.0, "Transfer", 2.0, "HighRisk", 2.0, "Cash", 3.0, 12.0,
        #      datetime.strptime("2017-06-10 11:30:27.000001", '%Y-%m-%d %H:%M:%S.%f'), "201706"),
        #     ("9", "Anshuman", "Prateek", "HighRisk", 5.0, "Deposit", 3.0, "Transfer", 2.0, "Cash", 5.0, "Credit", 5.0, 20.0,
        #      datetime.strptime("2017-01-10 11:31:27.948885", '%Y-%m-%d %H:%M:%S.%f'), "201901"),
        #     ("10", "Abhiroop", "Anshuman", "Transfer", 3.0, "Cash", 2.0, "HighRisk", 4.0, "Deposit", 4.0, "Cash", 1.0, 14.0,
        #      datetime.strptime("2018-01-11 19:01:17.123897", '%Y-%m-%d %H:%M:%S.%f'), "201801"),
        #     ("11", "Raksha", "Abhiroop", "Deposit", 2.0, "Credit", 4.0, "Transfer", 3.0, "HighRisk", 1.0, "Cash", 3.0, 13.0,
        #      datetime.strptime("2016-01-10 11:31:27.948887", '%Y-%m-%d %H:%M:%S.%f'), "201601"),
        #     ("12", "Prateek", "Raksha", "Credit", 3.0, "Cash", 5.0, "Transfer", 4.0, "Deposit", 2.0, "HighRisk", 2.0, 16.0,
        #      datetime.strptime("2018-03-20 13:41:57.949268", '%Y-%m-%d %H:%M:%S.%f'), "201803"),
        #     ("13", "Eren", "Nagato", "HighRisk", 4.0, "Deposit", 1.0, "Transfer", 2.0, "Credit", 4.0, "Cash", 4.0, 15.0,
        #      datetime.strptime("2017-02-23 15:15:51.948889", '%Y-%m-%d %H:%M:%S.%f'), "201702"),
        #     ("14", "Genos", "Eren", "HighRisk", 4.0, "Deposit", 2.0, "Credit", 3.0, "Transfer", 3.0, "Cash", 5.0, 17.0,
        #      datetime.strptime("2018-02-07 21:11:16.942360", '%Y-%m-%d %H:%M:%S.%f'), "201802")
        # }
        # cls.df = cls.spark.createDataFrame(data, cls.schema)
        # cls.df.show(truncate=False)

    def test_add_group(self):
        self.a1.add_group()
        test_group_path = self.cfg_parser.get('path', 'test_group_data')
        group_schema = StructType([
            StructField("REF_ID", StringType(), True),
            StructField("ORIG", StringType(), True),
            StructField("BENEF", StringType(), True),
            StructField("FEATURE1", StringType(), True),
            StructField("FEATURE1_Score", DoubleType(), True),
            StructField("FEATURE2", StringType(), True),
            StructField("FEATURE2_Score", DoubleType(), True),
            StructField("FEATURE3", StringType(), True),
            StructField("FEATURE3_Score", DoubleType(), True),
            StructField("FEATURE4", StringType(), True),
            StructField("FEATURE4_Score", DoubleType(), True),
            StructField("FEATURE5", StringType(), True),
            StructField("FEATURE5_Score", DoubleType(), True),
            StructField("TOTAL_Score", DoubleType(), True),
            StructField("PAYMENT_DATE", TimestampType(), True),
            StructField("Group", LongType(), True),
            StructField("MONTH", StringType(), True),
        ])
        test_group_df = self.spark.read.csv(test_group_path, header=True, schema=group_schema)
        cols = self.a1.df.columns[:]
        test_group_df = test_group_df.select(*cols)
        self.a1.df.printSchema()
        test_group_df.printSchema()
        self.dataframe_equal(self.a1.df, test_group_df, "REF_ID")

    def test_alert_key(self):
        self.a1.add_alert_key()
        test_alert_data = self.cfg_parser.get('path', 'test_alert_data')
        alert_schema = StructType([
            StructField("REF_ID", StringType(), True),
            StructField("ORIG", StringType(), True),
            StructField("BENEF", StringType(), True),
            StructField("FEATURE1", StringType(), True),
            StructField("FEATURE1_Score", DoubleType(), True),
            StructField("FEATURE2", StringType(), True),
            StructField("FEATURE2_Score", DoubleType(), True),
            StructField("FEATURE3", StringType(), True),
            StructField("FEATURE3_Score", DoubleType(), True),
            StructField("FEATURE4", StringType(), True),
            StructField("FEATURE4_Score", DoubleType(), True),
            StructField("FEATURE5", StringType(), True),
            StructField("FEATURE5_Score", DoubleType(), True),
            StructField("TOTAL_Score", DoubleType(), True),
            StructField("PAYMENT_DATE", TimestampType(), True),
            StructField("Group", LongType(), True),
            StructField("ALERT_KEY", StringType(), True),
            StructField("MONTH", StringType(), True)
        ])
        test_alert_df = self.spark.read.csv(test_alert_data, header=True, schema=alert_schema)
        cols = self.a1.df.columns[:]
        test_alert_df = test_alert_df.select(*cols)
        self.a1.df.printSchema()
        test_alert_df.printSchema()
        self.dataframe_equal(self.a1.df, test_alert_df, "REF_ID")

    def test_top_3_features(self):
        self.a1.add_top_features()
        test_top3 = self.cfg_parser.get('path', 'test_top_feat_data')
        top3_schema = StructType([
            StructField("REF_ID", StringType(), True),
            StructField("ORIG", StringType(), True),
            StructField("BENEF", StringType(), True),
            StructField("Top_feat1", StringType(), True),
            StructField("Top_feat1_score", DoubleType(), True),
            StructField("Top_feat2", StringType(), True),
            StructField("Top_feat2_score", DoubleType(), True),
            StructField("Top_feat3", StringType(), True),
            StructField("Top_feat3_score", DoubleType(), True),
            StructField("TOTAL_Score", DoubleType(), True),
            StructField("PAYMENT_DATE", TimestampType(), True),
            StructField("Group", LongType(), True),
            StructField("ALERT_KEY", StringType(), True),
            StructField("Alert_top_feat1", StringType(), True),
            StructField("Alert_top_feat1_score", DoubleType(), True),
            StructField("Alert_top_feat2", StringType(), True),
            StructField("Alert_top_feat2_score", DoubleType(), True),
            StructField("Alert_top_feat3", StringType(), True),
            StructField("Alert_top_feat3_score", DoubleType(), True),
            StructField("MONTH", StringType(), True)
        ])
        test_top3_df = self.spark.read.csv(test_top3, header=True, schema=top3_schema)
        cols = self.a1.df.columns[:]
        test_top3_df = test_top3_df.select(*cols)

        self.a1.df.printSchema()
        test_top3_df.printSchema()
        self.dataframe_equal(self.a1.df, test_top3_df, "REF_ID")
