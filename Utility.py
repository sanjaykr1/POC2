from pyspark.sql import SparkSession
import logging

from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType, IntegerType



class Utility:
    """
    Utility class for implementing common methods being used by other classes
    """
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        logging.basicConfig(
            format='%(asctime)s : %(levelname)s :%(name)s :%(message)s',
            datefmt="%m/%d/%Y %I:%H:%S %p",
            filename="logfile.log",
            filemode="w",
            level=logging.INFO
        )

        self.spark = SparkSession.builder.appName("POC2").master("local").getOrCreate()
        self.logger.info("Creating spark session from Utility class")

    def readfile(self, filename, schema):
        """
        Reads csv file into a dataframe and returns the dataframe
        :param schema: custom schema for data
        :param filename: .csv file which needs to be read
        :return: df
        """
        self.logger.info("Reading csv file with filename %s", filename)
        try:
            cust_schema = self.custom_schema(schema)
            df = self.spark.read.csv(filename, header=True, schema=cust_schema)
        except Exception as e:
            self.logger.exception("Unable to read file Exception %s occurred", e)
            print("Unable to read file due to exception %s. ", e)
        else:
            return df

    def writefile(self, df, filename):
        """
        Write dataframe to csv file with the given filename
        :param df: dataframe to be written
        :param filename: filename in which dataframe is written
        :return: None
        """
        self.logger.info("Writing dataframe to file %s", filename)
        try:
            df.write.partitionBy("MONTH").mode("overwrite"). \
                option("header", True). \
                option("inferSchema", True). \
                csv(filename)
        except Exception as e:
            self.logger.exception("Unable to save file Exception %s occurred", e)
            print("Unable to save file due to", e)

    def custom_schema(self, schema):
        """
        Convert schema string to StructType schema
        :param schema: custom schema string
        :return:
        """
        self.logger.info("Converting custom schema from config file to schema")
        val_types = {
            "StringType()": StringType(),
            "DoubleType()": DoubleType(),
            "TimestampType()": TimestampType(),
            "IntegerType()": IntegerType()
        }
        split_schema = schema.split(",")
        cust_sch = StructType()
        for x in split_schema:
            y = x.split(" ")
            cust_sch.add(y[0], val_types[y[1]], True)
        return cust_sch
