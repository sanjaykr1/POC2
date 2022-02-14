import os
import sys

import networkx
from networkx import Graph

from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.functions import struct, sort_array, array
from pyspark.sql.types import StringType, DoubleType, TimestampType, StructType, StructField, IntegerType
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
pandas_df = df2.toPandas()
orig = tuple(pandas_df['ORIG'])
benef = tuple(pandas_df['BENEF'])
graph1.add_edges_from(list(zip(orig, benef)))
l1 = {}
count = 1
for comp in networkx.connected_components(graph1):
    l1[count] = list(comp)
    count += 1
l2 = list(networkx.connected_components(graph1))
dict1 = [dict.fromkeys(a, b) for b, a in enumerate(l2)]
x = {k: v for x in dict1 for k, v in x.items()}
# print(x)

pandas_df['Group'] = pandas_df['ORIG'].map(x)
df2 = spark.createDataFrame(pandas_df)
# df2.show()
udf1 = f.udf(udf1, IntegerType())

df2 = df2.withColumn("Group", udf1(struct(df2["ORIG"])))
# df2 = df2.withColumn("Group", [k for k, v in l1 if v == df2["ORIG"]])
w1 = Window.partitionBy("Group")
w2 = Window.partitionBy("Group", "ALERT_KEY")
df3 = df2.withColumn("ALERT_KEY", f.when((f.col("TOTAL_Score") == f.max("TOTAL_Score").over(w1)), True).
                     otherwise(None))
df3 = df3.withColumn("ALERT_KEY", f.when((f.col("PAYMENT_DATE") == f.min("PAYMENT_DATE").over(w2)) &
                                         (f.col("ALERT_KEY") == True), True).
                     otherwise(None))
# df3.show(25, truncate=False)
df3 = df3.orderBy("Group")
cols = ["FEATURE1_Score", "FEATURE2_Score", "FEATURE3_Score", "FEATURE4_Score", "FEATURE5_Score"]
df4 = df3. \
    withColumn("Top_feat1_score", sort_array(array([f.col(x) for x in cols]), asc=False)[0]). \
    withColumn("Top_feat2_score", sort_array(array([f.col(x) for x in cols]), asc=False)[1]). \
    withColumn("Top_feat3_score", sort_array(array([f.col(x) for x in cols]), asc=False)[2])
# df4.show()

df_dict = [row.asDict() for row in df3.collect()]
new_dict = []
for items in df_dict:
    values_list = list(items.values())
    values = values_list[3:-5]
    dicts = dict(zip(values[::2], values[1::2]))
    # print(new_dict)
    new_dict.append(dicts)
print(new_dict)

# df4.printSchema()
df5 = df4.withColumn("Top_feat1", f.when(df4["Top_feat1_score"] == [x for y in new_dict for x in y.values()])).\
    withColumn("Top_feat2", f.when(df4["Top_feat2_score"] == [x for y in new_dict for x in y.values()])). \
    withColumn("Top_feat3", f.when(df4["Top_feat3_score"] == [x for y in new_dict for x in y.values()]))
df5.show()

# dict2 = df3.select(df3[3], df3[4]).rdd.collectAsMap()
# print(dict2)

