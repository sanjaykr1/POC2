import os
import sys
import networkx
from networkx import Graph
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.functions import struct, desc
from pyspark.sql.types import StringType, DoubleType, TimestampType, StructType, StructField, IntegerType, ArrayType
from pyspark.sql.window import Window

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


def udf1(row):
    for k, v in l1.items():
        if row['ORIG'] in v:
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
print(l1)


l2 = list(networkx.connected_components(graph1))
dict1 = [dict.fromkeys(a, b) for b, a in enumerate(l2)]
x = {k: v for x in dict1 for k, v in x.items()}
print(x)

pandas_df['Group'] = pandas_df['ORIG'].map(x)
df2 = spark.createDataFrame(pandas_df)
# df2.show()
udf1 = f.udf(udf1, IntegerType())

dfy = df2.withColumn("Group", udf1(struct(df2["ORIG"])))
dfy.show()

# df2 = df2.withColumn("Group", [k for k, v in l1 if v == df2["ORIG"]])
w1 = Window.partitionBy("Group")
w3 = Window.partitionBy("Group").orderBy(desc("TOTAL_Score"), "PAYMENT_DATE")
df3 = df2.withColumn("ALERT_KEY", f.first(df2["ORIG"]).over(w3))

"""
w2 = Window.partitionBy("Group", "ALERT_KEY")
df3 = df2.withColumn("ALERT_KEY", f.when((f.col("TOTAL_Score") == f.max("TOTAL_Score").over(w1)), True).
                     otherwise(None))
df3.show()
df3 = df3.withColumn("ALERT_KEY", f.when((f.col("PAYMENT_DATE") == f.min("PAYMENT_DATE").over(w2)) &
                                         (f.col("ALERT_KEY") == True), df2["ORIG"]).
                     otherwise(None))
"""

df3 = df3.withColumn("ALERT_KEY", f.last("ALERT_KEY", True).over(w3))
# df3 = df3.orderBy(desc("TOTAL_Score"), "Group")
df3 = df3.orderBy("Group", "REF_ID")
# df3.show()
# dfx.show()
# cols = ["FEATURE1_Score", "FEATURE2_Score", "FEATURE3_Score", "FEATURE4_Score", "FEATURE5_Score"]
"""
df4 = df3. \
    withColumn("Top_feat1_score", sort_array(array([f.col(x) for x in cols]), asc=False)[0]). \
    withColumn("Top_feat2_score", sort_array(array([f.col(x) for x in cols]), asc=False)[1]). \
    withColumn("Top_feat3_score", sort_array(array([f.col(x) for x in cols]), asc=False)[2])
# df4.show()
"""
df_xyz = df3.select(df3.columns[3:-5])
df_dict = [row.asDict() for row in df_xyz.collect()]
new_dict = []
for items in df_dict:
    values_list = list(items.values())
    x = dict(zip(values_list[::2], values_list[1::2]))
    dicts = dict(sorted(x.items(), reverse=True, key=lambda item: item[1]))
    print(dicts)
    new_dict.append([ele for ele in list(dicts.items())[:3]])
    # new_dict.append(dicts)
print(new_dict)
# top3 = []
# for a in new_dict:
#    top3.append([list(ele) for ele in list(a.items())[:3]])
# print(top3)
"""
top3_1 = []
for a in new_dict:
    top3_1.append(dict(list(a.items())[:3]))
print(top3_1)
"""
cols_list = df3.rdd.map(lambda x: x[0]).collect()
print(cols_list)
"""
cols_dict = dict(zip(cols_list, top3_1))

"""
schema = StructType([
    StructField("Feat1", ArrayType(StringType(), True), True),
    StructField("Feat2", ArrayType(StringType(), True), True),
    StructField("Feat3", ArrayType(StringType(), True), True)
])
"""
sch = StructType([
    StructField("Dict1", MapType(StringType(), DoubleType(), False), True)
])
print(top3_1[0])
xyz = spark.createDataFrame([top3_1[0]], schema=sch)
xyz.printSchema()
xyz.show()
"""
df5 = spark.createDataFrame(new_dict, schema=schema)
# df5.printSchema()
# df5.show()
# temp_df = spark.createDataFrame(top3)
# temp_df = temp_df.withColumn("REF_ID", xyz["REF_ID"])
df5 = df5.select(
    df5.Feat1[0].alias("Top_feat1"),
    df5.Feat1[1].cast(DoubleType()).alias("Top_feat1_score"),
    df5.Feat2[0].alias("Top_feat2"),
    df5.Feat2[1].cast(DoubleType()).alias("Top_feat2_score"),
    df5.Feat3[0].alias("Top_feat3"),
    df5.Feat3[1].cast(DoubleType()).alias("Top_feat3_score"))
# df5.show()
w = Window().orderBy(f.lit('A'))


def add_labels(i):
    return cols_list[i-1]
    # since row num begins from 1


labels_udf = f.udf(add_labels, StringType())

# df3 = df3.withColumn("row", f.row_number().over(w))
df5 = df5.withColumn("row", f.row_number().over(w))
new_df = df5.withColumn('REF_ID', labels_udf('row'))
new_df = new_df.drop("row")
new_df.show()

df3 = df3.join(new_df, ["REF_ID"])
df3 = df3.drop("row")
df3 = df3.orderBy("Group", "REF_ID")
df3.show()

df4 = df3.withColumn("Alert_top_feat1", f.first(df3["Top_feat1"]).over(w3)). \
    withColumn("Alert_top_feat1_score", f.first(df3["Top_feat1_score"]).over(w3)). \
    withColumn("Alert_top_feat2", f.first(df3["Top_feat2"]).over(w3)). \
    withColumn("Alert_top_feat2_score", f.first(df3["Top_feat2_score"]).over(w3)). \
    withColumn("Alert_top_feat3", f.first(df3["Top_feat3"]).over(w3)). \
    withColumn("Alert_top_feat3_score", f.first(df3["Top_feat3_score"]).over(w3))

df4.show()
drop_cols = ["FEATURE1", "FEATURE1_Score", "FEATURE2", "FEATURE2_Score", "FEATURE3", "FEATURE3_Score", "FEATURE4",
             "FEATURE4_Score", "FEATURE5", "FEATURE5_Score"]
df5 = df4.drop(*drop_cols)
select_cols = ["REF_ID", "ORIG", "BENEF", "Top_feat1", "Top_feat1_score", "Top_feat2", "Top_feat2_score",
               "Top_feat3", "Top_feat3_score", "TOTAL_Score", "PAYMENT_DATE", "MONTH", "group", "ALERT_KEY",
               "Alert_top_feat1", "Alert_top_feat1_score", "Alert_top_feat2",
               "Alert_top_feat2_score", "Alert_top_feat3", "Alert_top_feat3_score"]
df5 = df5.select(*select_cols)
df5.printSchema()
df5 = df5.orderBy("Group","REF_ID")
df5.show()
