from pandas.core.interchange.dataframe_protocol import DataFrame
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from collections import Counter


class Processor:
    def __init__(self,df:DataFrame):
        self.df = df
        self.new_df = df.copy()


    def add_new_column_rarest_word(self):
        list_rarest_word = []
        col_text = self.new_df["Text"]

        for field_text in col_text:
            count = Counter(field_text.split(" "))
            res = [word for word in count if count[word] == min(count.values())]
            list_rarest_word.append(res[0])

        #print(f"{len(list_rarest_word)}")
        self.new_df["rarest_word"] = list_rarest_word
        #print(self.new_df)
        return  self.new_df

    def add_new_column_emotion(self):
        nltk.download('vader_lexicon')  # Compute sentiment labels
        emotion_list = []
        col_text = self.new_df["Text"]
        for field_text in col_text:
            tweet = field_text
            score = SentimentIntensityAnalyzer().polarity_scores(tweet)
            if 1 >= score['compound'] >= 0.5:
                emot = "positive"
            elif  0.49 >= score['compound'] >= -0.49:
                emot = "neutral"
            else:
                emot = "negative"

            emotion_list.append(emot)
        #print(len(emotion_list))
        self.new_df["emotion"] = emotion_list
        print(self.new_df)
        #return self.new_df







if __name__ == "__main__":

    from fetcher import Connector,DAL

    connector = Connector("mongodb+srv://IRGC:iraniraniran@iranmaldb.gurutam.mongodb.net/")
    dal = DAL(connector)
    df = dal.get_mongoDB_as_df()
    processor = Processor(df)
    processor.add_new_column_rarest_word()
    processor.add_new_column_emotion()


