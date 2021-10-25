import pandas as pd
import regex as re

df = pd.read_csv("Dataset_all_Filtered_1.csv", index_col=None, header=0)
for idx, tweet in df.iterrows():
    df.loc[idx, "tweet_text"] = ' '.join(
        re.sub("(@[A-Za-z0-9_]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", tweet.tweet_text).split())

df.to_csv("Dataset_all_text_filter.csv", index=False)
