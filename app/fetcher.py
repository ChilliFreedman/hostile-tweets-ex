from pymongo import MongoClient
class Connector:
    def __init__(self,connection_string,user = "IRGC",passw = "iraniraniran",dbname = "IranMalDB"):
        self.user = user
        self.passw = passw
        self.dbname = dbname
        #self.connection_string = f"mongodb+srv://{self.user}:{self.passw}@{self.dbname}.gurutam.mongodb.net/"
        self.connection_string = connection_string
        self.client = MongoClient(self.connection_string)
        self.db = self.client[dbname]


    def get_db(self):
        return self.db

    def close(self):
        self.client.close()

import pandas as pd

class DAL:
    def __init__(self,connector : Connector,collection_name="tweets"):
        self.collection = connector.get_db()[collection_name]

    def get_all_data(self):
        cursor = self.collection.find()
        for item in cursor:
            print(item)

    def get_mongoDB_as_df(self):
        df = pd.DataFrame(self.collection.find())
        return df


if __name__ == "__main__":
    connector = Connector("mongodb+srv://IRGC:iraniraniran@iranmaldb.gurutam.mongodb.net/")
    dal = DAL(connector)
    #dal.get_all_data()
    #print(dal.get_mongoDB_as_df())