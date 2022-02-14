import networkx
from networkx import Graph

from Utility import Utility
from pyspark.sql import functions as f, Window
from pyspark.sql.types import IntegerType


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
        # super().spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
        self.filename = filename
        self.schema = schema
        self.df = super().readfile(self.filename, self.schema)
        self.l1 = {}
        self.count = 1
        self.df.show()

    def add_group(self):
        """
        method to implement grouping according to connected components
        :return:
        """
        self.df = self.df.filter(f.col("TOTAL_Score") > 15)
        graph1 = Graph()
        pandas_df = self.df.toPandas()
        orig = tuple(pandas_df['ORIG'])
        benef = tuple(pandas_df['BENEF'])
        graph1.add_edges_from(list(zip(orig, benef)))
        l2 = list(networkx.connected_components(graph1))
        dict1 = [dict.fromkeys(a, b) for b, a in enumerate(l2)]
        x = {k: v for x in dict1 for k, v in x.items()}
        pandas_df['Group'] = pandas_df['ORIG'].map(x)
        self.df = self.spark.createDataFrame(pandas_df)
        self.df.show()
        self.writefile(self.df, "grouped_data")

    def add_alert_key(self):
        """
        Method to implement Alert Key column
        :return:
        """
        w1 = Window.partitionBy("Group")
        w2 = Window.partitionBy("Group", "ALERT_KEY")
        self.df = self.df.withColumn("ALERT_KEY", f.when((f.col("TOTAL_Score") == f.max("TOTAL_Score").over(w1)),
                                                         True).otherwise(None))
        self.df = self.df.withColumn("ALERT_KEY", f.when((f.col("PAYMENT_DATE") == f.min("PAYMENT_DATE").over(w2)) &
                                                         (f.col("ALERT_KEY") == True), self.df["ORIG"]).
                                     otherwise(None))
        self.df = self.df.orderBy("Group")
        self.df.show(25)
