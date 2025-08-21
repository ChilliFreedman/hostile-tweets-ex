from fetcher import Connector,DAL
from processor import Processor
class Maneser:
    def __init__(self):
        self.connector = Connector("mongodb+srv://IRGC:iraniraniran@iranmaldb.gurutam.mongodb.net/")
        self.dal = DAL(self.connector)
        self.df = self.dal.get_mongoDB_as_df()
        self.processor = Processor(self.df)
        self.df = None

    def run_processor(self):

        self.processor.add_new_column_rarest_word()
        self.processor.add_new_column_emotion()
        self.processor.add_new_column_weapons_detected()
        self.df = self.processor.get_new_df()

    def convert_to_json(self):
        dict_df = self.df.to_dict(orient = "records")
        return dict_df


if __name__ == "__main__":
    man = Maneser()
    man.run_processor()
    print(man.convert_to_json())