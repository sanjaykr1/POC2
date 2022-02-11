from pyspark.sql import SparkSession
from pyspark.sql.functions import struct
from pyspark.sql.types import *
from pyspark.sql import functions as f
import networkx
from networkx import Graph


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
pandas_df = df2.toPandas()
orig = tuple(pandas_df['ORIG'])
benef = tuple(pandas_df['BENEF'])
graph1.add_edges_from(list(zip(orig, benef)))
l1 = {}
count = 0
for comp in networkx.connected_components(graph1):
    l1[count] = list(comp)
    count += 1

udf1 = f.udf(udf1, IntegerType())
df2 = df2.withColumn("Group", udf1(struct(df2["ORIG"])))
df2.show(25, truncate=False)
