import os
import sys

import networkx
from networkx import Graph

from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.functions import struct
from pyspark.sql.types import *
from pyspark.sql.window import Window


os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


def udf1(row):
    for k, v in l1.items():
        if row['ORIG'] in v:
            # print(k)
            return k


new_schema = StructType([
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
    StructField("MONTH", StringType(), True),
])

spark = SparkSession.builder.appName("POC2").master("local").getOrCreate()
df1 = spark.read.option("header", True).csv("dataset/data.csv", schema=new_schema)
# df1.show(truncate=False)

df2 = df1.filter(f.col("TOTAL_Score") > 15)
# print(df2.count())

graph1 = Graph()
pandas_df = df1.toPandas()
orig = tuple(pandas_df['ORIG'])
benef = tuple(pandas_df['BENEF'])
graph1.add_edges_from(list(zip(orig, benef)))
l1 = {}
count = 1
for comp in networkx.connected_components(graph1):
    l1[count] = list(comp)
    count += 1
    print(comp)

udf1 = f.udf(udf1, IntegerType())
df2 = df1.withColumn("Group", udf1(struct(df1["ORIG"])))

w1 = Window.partitionBy("Group")
df3 = df2.withColumn("ALERT_KEY", f.when((f.col("TOTAL_Score") == f.max("TOTAL_Score").over(w1)) |
                                         (f.col("PAYMENT_DATE") >= f.min("PAYMENT_DATE").over(w1)), "ALERT_KEY_TRUE").
                     otherwise(None))
df3.show(25, truncate=False)
