import configparser

from accounts import Account


class Pipeline(Account):
    """
    class for running pipeline tasks
    """

    def __init__(self, filename, schema):
        """
        Creating account class object
        :param filename: dataset filename to be read
        :param schema: custom schema for the dataset
        """
        self.acc = Account(filename, schema)

    def account_methods(self):
        """
        Method to Perform aggregations on Account data
        :return:
        """
        self.acc.add_group()
        self.acc.add_alert_key()
        self.acc.add_top_features()


cfg_parser = configparser.ConfigParser()
filepath = "config.cfg"
cfg_parser.read(filepath)
file = cfg_parser.get('path', 'dataset')
sch = cfg_parser.get('schema', 'custom_schema')
p1 = Pipeline(file, sch)
p1.account_methods()
