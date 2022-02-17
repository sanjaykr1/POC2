import networkx
from networkx import Graph

from Utility import Utility
from pyspark.sql import functions as f, Window
from pyspark.sql.types import StructType, StructField, ArrayType, StringType, DoubleType, IntegerType, LongType


class Account(Utility):
    """
    Account class for initializing the account data
    """

    def __init__(self, filename, schema):
        """
        initialize Account object with custom schema and specific filename
        :param filename: dataset file
        :param schema: custom schema
        """
        super().__init__()
        self.logger.info("Creating account class object.")
        # super().spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
        self.filename = filename
        self.schema = schema
        self.df = super().readfile(self.filename, self.schema)
        self.df.show()

    def add_group(self):
        """
        method to implement grouping according to connected components
        :return:
        """
        self.logger.info("Grouping the data according to connected components.")
        self.df = self.df.filter(f.col("TOTAL_Score") > 15)

        graph1 = Graph()
        pandas_df = self.df.toPandas()
        orig = tuple(pandas_df['ORIG'])
        benef = tuple(pandas_df['BENEF'])
        graph1.add_edges_from(list(zip(orig, benef)))
        # l2 = list(networkx.connected_components(graph1))
        # dict1 = [dict.fromkeys(a, b) for b, a in enumerate(l2)]
        # x = {k: v for x in dict1 for k, v in x.items()}
        # pandas_df['Group'] = pandas_df['ORIG'].map(x)

        l1 = {}
        count = 0

        def udf1(row):
            for k, v in l1.items():
                if row['ORIG'] in v:
                    return k

        for comp in networkx.connected_components(graph1):
            l1[count] = list(comp)
            count += 1
        udf1 = f.udf(udf1, LongType())
        # self.df = self.spark.createDataFrame(pandas_df)
        self.df = self.df.withColumn("Group", udf1(f.struct(self.df["ORIG"])))
        self.df = self.df.orderBy("Group", "REF_ID")
        self.df.show()
        self.writefile(self.df, "grouped_data")

    def add_alert_key(self):
        """
        Method to implement Alert Key column
        :return:
        """
        self.logger.info("Creating alert key for each group based on Max Total score and least payment date.")
        w1 = Window.partitionBy("Group").orderBy(f.desc("TOTAL_Score"), "PAYMENT_DATE")
        self.df = self.df.withColumn("ALERT_KEY", f.first(self.df["ORIG"]).over(w1))
        self.df = self.df.withColumn("ALERT_KEY", f.last("ALERT_KEY", True).over(w1))
        self.df = self.df.orderBy("Group", "REF_ID")
        # self.df.show(25)
        self.writefile(self.df, "alert_key_data")

    def add_top_features(self):
        """
        Method to sort top 3 features from 5 features
        :return:
        """
        self.logger.info("Sorting top 3 features from 5 features for each record.")
        df_xyz = self.df.select(self.df.columns[3:-5])
        df_dict = [row.asDict() for row in df_xyz.collect()]
        """
        Sorting out features in descending order
        """
        new_dict = []
        for items in df_dict:
            values_list = list(items.values())
            x = dict(zip(values_list[::2], values_list[1::2]))
            dicts = dict(sorted(x.items(), reverse=True, key=lambda item: item[1]))
            new_dict.append([ele for ele in list(dicts.items())[:3]])
        """
        The above code snipped generates a list of lists having top 3 as follows
        [[('Credit', 5.0), ('Transfer', 4.0), ('HighRisk', 3.0)], [('HighRisk', 5.0), ('Cash', 5.0), ('Credit', 5.0)]..
        (Feature name, feature score)
        """
        """
        Extracting top 3 features
        """
        schema = StructType([
            StructField("Feat1", ArrayType(StringType(), True), True),
            StructField("Feat2", ArrayType(StringType(), True), True),
            StructField("Feat3", ArrayType(StringType(), True), True)
        ])
        df1 = self.spark.createDataFrame(new_dict, schema=schema)
        df1 = df1.select(
            df1.Feat1[0].alias("Top_feat1"),
            df1.Feat1[1].cast(DoubleType()).alias("Top_feat1_score"),
            df1.Feat2[0].alias("Top_feat2"),
            df1.Feat2[1].cast(DoubleType()).alias("Top_feat2_score"),
            df1.Feat3[0].alias("Top_feat3"),
            df1.Feat3[1].cast(DoubleType()).alias("Top_feat3_score"))

        def add_labels(i):
            """
            UDF for adding ref_id column to top_3 features dataframe
            :param i: index of current row
            :return:
            """
            return cols_list[i - 1]

        w = Window().orderBy(f.lit('row_no'))

        cols_list = self.df.rdd.map(lambda x: x[0]).collect()
        labels_udf = f.udf(add_labels, StringType())

        df1 = df1.withColumn("row", f.row_number().over(w))
        df1 = df1.withColumn('REF_ID', labels_udf('row'))
        df1 = df1.drop("row")

        self.df = self.df.join(df1, ["REF_ID"])

        w = Window.partitionBy("Group").orderBy(f.desc("TOTAL_Score"), "PAYMENT_DATE")
        self.df = self.df.withColumn("Alert_top_feat1", f.first(self.df["Top_feat1"]).over(w)). \
            withColumn("Alert_top_feat1_score", f.first(self.df["Top_feat1_score"]).over(w)). \
            withColumn("Alert_top_feat2", f.first(self.df["Top_feat2"]).over(w)). \
            withColumn("Alert_top_feat2_score", f.first(self.df["Top_feat2_score"]).over(w)). \
            withColumn("Alert_top_feat3", f.first(self.df["Top_feat3"]).over(w)). \
            withColumn("Alert_top_feat3_score", f.first(self.df["Top_feat3_score"]).over(w))

        drop_cols = ["FEATURE1", "FEATURE1_Score", "FEATURE2", "FEATURE2_Score", "FEATURE3", "FEATURE3_Score",
                     "FEATURE4", "FEATURE4_Score", "FEATURE5", "FEATURE5_Score"]
        self.df = self.df.drop(*drop_cols)
        select_cols = ["REF_ID", "ORIG", "BENEF", "Top_feat1", "Top_feat1_score", "Top_feat2", "Top_feat2_score",
                       "Top_feat3", "Top_feat3_score", "TOTAL_Score", "PAYMENT_DATE", "MONTH", "Group", "ALERT_KEY",
                       "Alert_top_feat1", "Alert_top_feat1_score", "Alert_top_feat2",
                       "Alert_top_feat2_score", "Alert_top_feat3", "Alert_top_feat3_score"]
        self.df = self.df.select(*select_cols)
        self.df = self.df.orderBy("Group", "REF_ID")
        self.df.show()
        self.writefile(self.df, "top_features")
