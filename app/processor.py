from pandas.core.interchange.dataframe_protocol import DataFrame
from collections import Counter


class Processor:
    def __init__(self,df:DataFrame):
        self.df = df
        self.new_df = df.copy()

    def add_new_column_rarest_word(self):
        list_rarest_word = []
        col_text = self.new_df["Text"]

        for aaa in col_text:
            count = Counter(aaa.split(" "))
            res = [word for word in count if count[word] == min(count.values())]
            list_rarest_word.append(res[0])

        #print(f"{len(list_rarest_word)}")
        self.new_df["rarest_word"] = list_rarest_word
        #print(self.new_df)
        return  self.new_df





if __name__ == "__main__":

    from fetcher import Connector,DAL

    connector = Connector("mongodb+srv://IRGC:iraniraniran@iranmaldb.gurutam.mongodb.net/")
    dal = DAL(connector)
    df = dal.get_mongoDB_as_df()
    processor = Processor(df)
    print(processor.add_new_column_rarest_word())

