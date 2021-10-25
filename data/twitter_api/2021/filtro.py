import pandas as pd
import glob

all_files = glob.glob("*.csv")

li = []

us_state_to_abbrev = {
    "Alabama": "AL",
    "Alaska": "AK",
    "Arizona": "AZ",
    "Arkansas": "AR",
    "California": "CA",
    "Colorado": "CO",
    "Connecticut": "CT",
    "Delaware": "DE",
    "Florida": "FL",
    "Georgia": "GA",
    "Hawaii": "HI",
    "Idaho": "ID",
    "Illinois": "IL",
    "Indiana": "IN",
    "Iowa": "IA",
    "Kansas": "KS",
    "Kentucky": "KY",
    "Louisiana": "LA",
    "Maine": "ME",
    "Maryland": "MD",
    "Massachusetts": "MA",
    "Michigan": "MI",
    "Minnesota": "MN",
    "Mississippi": "MS",
    "Missouri": "MO",
    "Montana": "MT",
    "Nebraska": "NE",
    "Nevada": "NV",
    "New Hampshire": "NH",
    "New Jersey": "NJ",
    "New Mexico": "NM",
    "New York": "NY",
    "North Carolina": "NC",
    "North Dakota": "ND",
    "Ohio": "OH",
    "Oklahoma": "OK",
    "Oregon": "OR",
    "Pennsylvania": "PA",
    "Rhode Island": "RI",
    "South Carolina": "SC",
    "South Dakota": "SD",
    "Tennessee": "TN",
    "Texas": "TX",
    "Utah": "UT",
    "Vermont": "VT",
    "Virginia": "VA",
    "Washington": "WA",
    "West Virginia": "WV",
    "Wisconsin": "WI",
    "Wyoming": "WY",
    "District of Columbia": "DC",
    "American Samoa": "AS",
    "Guam": "GU",
    "Northern Mariana Islands": "MP",
    "Puerto Rico": "PR",
    "United States Minor Outlying Islands": "UM",
    "U.S. Virgin Islands": "VI"
}

abbrev_to_us_state = dict(map(reversed, us_state_to_abbrev.items()))

for filename in all_files:
    s = ""
    if "Astrazeneca" in filename:
        s = "Astrazeneca"
    elif "AstraZeneca" in filename:
        s = "AstraZeneca"
    elif "J&J" in filename:
        s = "J&J"
    elif "Moderna" in filename:
        s = "Moderna"
    elif "Pfizer" in filename:
        s = "Pfizer"
    elif "effects" in filename:
        s = "effects"
    df = pd.read_csv(filename, index_col=None, header=0)
    loc = ""
    for idx, tweet in df.iterrows():
        print(tweet.location, tweet.tweet_id, filename)
        if "USA" in str(tweet.location):
            # print(loc, tweet.location)
            loc = str(tweet.location)[0:str(tweet.location).find(',')]
            df.loc[idx, "location"] = loc
        else:
            loc = str(tweet.location)[str(tweet.location).find(',') + 2:]
            # print(loc, tweet.location)
            if loc in abbrev_to_us_state.keys():
                n = abbrev_to_us_state[loc]
                df.loc[idx, "location"] = n

    df["vaccine"] = s
    li.append(df)

frame = pd.concat(li, axis=0, ignore_index=True)
frame.to_csv("Dataset_all.csv", index=False)

'''Frecuencia de tweet por dia, wordcloud, NRC-Emotion-Lexicon. LDA model trained using the english based corpus, sentimiento por vacuna
 wordcloud por vacuna, 2-gram graph, replicar analisis por estado'''
