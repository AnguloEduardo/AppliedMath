#!/usr/bin/env python

# Copyright 2021 Huawei BNC LATAM.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""This application extracts twitter data using tweepy."""


def read_api_keys(file):
    """
    File format: .json
    schema:
            {
            "API_KEY":"",
            "API_SECRET_KEY":"",
            "ACCESS_TOKEN":"",
            "ACCESS_TOKEN_SECRET":""
            }
    """

    import json
    with open(file) as f:
        jsonstr = json.load(f)

    return jsonstr['API_KEY'], jsonstr['API_SECRET_KEY'], jsonstr['ACCESS_TOKEN'], jsonstr['ACCESS_TOKEN_SECRET']


def check_todays_dated_folder_existance(warehouse_path):
    import os
    import datetime
    todays_date = datetime.date.today()
    dated_folder = warehouse_path + str(todays_date.year) + '/' + str(todays_date) + '/'
    folder_exists = os.path.exists(dated_folder)
    print(dated_folder, ' [...folder already exists?]: ', folder_exists)
    return folder_exists


def check_yesterdays_dated_folder_existance(warehouse_path):
    import os
    import datetime
    from datetime import timedelta
    yesterdays_date = datetime.date.today() - timedelta(days=1)
    dated_folder = warehouse_path + str(yesterdays_date.year) + '/' + str(yesterdays_date) + '/'
    folder_exists = os.path.exists(dated_folder)
    print(dated_folder, ' [...folder already exists?]: ', folder_exists)
    return folder_exists


def check_dated_folder_existance(warehouse_path, extract_data_from):
    import os
    dated_folder = warehouse_path + str(extract_data_from.year) + '/' + str(extract_data_from) + '/'
    folder_exists = os.path.exists(dated_folder)
    print(dated_folder, ' [...folder already exists?]: ', folder_exists)
    return folder_exists


def extract_twitter_data(warehouse_path, extract_data_from):
    import datetime
    from datetime import timedelta
    import os

    todays_date = datetime.date.today()
    print('Extraction date attempt : ', extract_data_from)
    print("Today's date : ", todays_date)

    #  Conditions ...
    if extract_data_from == 'today':
        folder_exist = check_todays_dated_folder_existance(warehouse_path)
        if folder_exist is False:
            dated_folder = warehouse_path + str(todays_date.year) + '/' + str(todays_date) + '/'
        elif folder_exist is True:
            raise ValueError("The folder already exist!")

        search_date = todays_date

    elif extract_data_from == 'yesterday':
        folder_exist = check_yesterdays_dated_folder_existance(warehouse_path)
        if folder_exist is False:
            yesterdays_date = todays_date - timedelta(days=1)
            dated_folder = warehouse_path + str(yesterdays_date.year) + '/' + str(yesterdays_date) + '/'
        elif folder_exist is True:
            raise ValueError("The folder already exist!")

        search_date = yesterdays_date

    else:
        folder_exist = check_dated_folder_existance(warehouse_path, extract_data_from)
        days_elapsed = todays_date - extract_data_from
        print("Days elapsed : ", days_elapsed)

        if folder_exist is False:
            dated_folder = warehouse_path + str(extract_data_from.year) + '/' + str(extract_data_from) + '/'
        elif folder_exist is True:
            raise ValueError("The folder already exist!")

        if days_elapsed < datetime.timedelta(days=0):
            raise ValueError("Can't extract tweets from the future!")
        elif days_elapsed > datetime.timedelta(days=7):
            raise ValueError(
                "Extraction date limit exceeded!, the free twitter account only let us collect data from the last seven days, try again!")

        search_date = extract_data_from

    #  Extraction
    os.makedirs(dated_folder)
    print('New folder created at: ', dated_folder)
    print("Request accepted, startig extraction ...")
    start_extraction(search_date, dated_folder)


def start_extraction(search_date, destination_folder):
    import time as tm
    import pandas as pd
    import tweepy
    from datetime import datetime, timedelta

    API_KEY, API_SECRET_KEY, ACCESS_TOKEN, ACCESS_TOKEN_SECRET = read_api_keys('secrets/twitter_credentials.json')

    # Get API access
    auth = tweepy.OAuthHandler(API_KEY, API_SECRET_KEY)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
    api = tweepy.API(auth)

    # Search configuration
    until_date = str(search_date + timedelta(days=1))
    print('Search range: from', search_date, ' to ', until_date)
    print()

    # Collect N Tweets with id, time, text, user data, likes, retweets and location (when available) FOR EACH OPERATOR
    # Create empty lists
    tweet_id = []
    # tweet_id_str = []
    tweet_time = []
    tweet_text = []
    # user_id = []
    # user_name = []
    # user_screen_name = []
    # likes = []
    # retweets = []
    # followers = []
    # in_reply_to_screen_name = []
    # in_reply_to_status_id_str = []
    location = []

    places = api.geo_search(query="USA", granularity="country")
    place_id = places[0].id
    language = 'en'
    search_terms = ['Moderna vaccine', 'Pfizer vaccine', 'J&J vaccine', 'Astrazeneca vaccine', 'Sinovac vaccine',
                   'Sputnik vaccine', 'Cansino vaccine', 'Sinopharm vaccine', 'vaccine side effects']
    number_of_tweets = 100  # you decide the limit of tweets to extract per day
    until_date = str(search_date + timedelta(days=1))
    for index in range(len(search_terms)):
        query = '{} place:{}'.format(search_terms[index], place_id)
        print('timestamp: ', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        print('Querying for ' + places[0].country_code + ' on ' + str(search_date) + ' with ' + query)

        cursor = tweepy.Cursor(api.search, since_id=None, q=query, lang=language,
                               until=until_date, tweet_mode='extended', wait_on_rate_limit=True). \
            items(number_of_tweets)

        while True:
            try:
                i = cursor.next()
                if i.created_at.date() == search_date:
                    tweet_id.append(i.id)
                    # tweet_id_str.append(i.id_str)
                    tweet_time.append(i.created_at)
                    tweet_text.append(i.full_text)
                    # user_id.append(i.user.id)
                    # user_name.append(i.user.name)
                    # user_screen_name.append(i.user.screen_name)
                    # likes.append(i.favorite_count)
                    # retweets.append(i.retweet_count)
                    # followers.append(i.user.followers_count)
                    # in_reply_to_screen_name.append(i.in_reply_to_screen_name)
                    # in_reply_to_status_id_str.append(i.in_reply_to_status_id_str)

                    if i.place is not None:
                        location.append(i.place.full_name)
                    elif i.user.location != "":
                        location.append(i.user.location)
                    else:
                        location.append('No defined')
                else:
                    break
            except tweepy.TweepError as e:
                print(datetime.now())
                print('Twitter error: ', e.args)
                print('PAUSED --> On ' + search_terms[index] + ' collecting ' + str(
                    len(tweet_id)) + ' tweets and CONTINUING')
                tm.sleep(60 * 15)
                continue
            except StopIteration:
                break

        # If len(list) > number_of_tweets --> no fue suficiente, hay que buscar de nuevo con otro limite.
        print('timestamp: ', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        print('For ' + search_terms[index] + ' collected ' + str(len(tweet_id)) + ' tweets')
        print()
        print()

        df = pd.DataFrame({
            'tweet_id': tweet_id,
            # 'tweet_id_str': tweet_id_str,
            'tweet_time': tweet_time,
            'tweet_text': tweet_text,
            # 'user_id': user_id,
            # 'user_name': user_name,
            # 'user_screen_name': user_screen_name,
            'location': location,
            # 'likes': likes,
            # 'retweets': retweets,
            # 'followers': followers,
            # 'in_reply_to_screen_name': in_reply_to_screen_name,
            # 'in_reply_to_status_id_str': in_reply_to_status_id_str,
        })

        #  Replace \n by space, to remove line jumps
        df = df.replace('\n', '', regex=True)

        #  Save DataFrame as .csv
        df.to_csv(destination_folder + places[0].country_code + '_' + search_terms[index] + '_' +
                  str(search_date).replace('-', '_') + '.csv', index=False, encoding='utf_8_sig', sep='\t')


def filter_tweets():
    pass


if __name__ == '__main__':

    import argparse
    import datetime

    print('[ --- Huawei - BNC LATAM --- ]')
    welcome_message = """
  _____          _ _   _              _____      _                  _   _
 |_   _|_      _(_) |_| |_ ___ _ __  | ____|_  _| |_ _ __ __ _  ___| |_(_) ___  _ __
   | | \ \ /\ / / | __| __/ _ \ '__| |  _| \ \/ / __| '__/ _` |/ __| __| |/ _ \| '_ \ 
   | |  \ V  V /| | |_| ||  __/ |    | |___ >  <| |_| | | (_| | (__| |_| | (_) | | | |
   |_|   \_/\_/ |_|\__|\__\___|_|    |_____/_/\_\\__|_|  \__,_|\___|\__|_|\___/|_| |_|

    """
    # made with: http://patorjk.com/software/taag/#p=display&f=Graffiti&t=Type%20Something%20
    print(welcome_message)
    print('Loading ...')

    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    subparsers = parser.add_subparsers(dest='command')

    dated_folder_existance = subparsers.add_parser(
        'check-for-folder-existance',
        help=check_dated_folder_existance.__doc__)

    dated_folder_existance.add_argument(
        'date',
        type=datetime.date.fromisoformat,
        help='Date format required: YYYY-MM-DD')

    todays_dated_folder_existance = subparsers.add_parser(
        'check-for-todays-folder-existance',
        help=check_yesterdays_dated_folder_existance.__doc__)

    yesterdays_dated_folder_existance = subparsers.add_parser(
        'check-for-yesterdays-folder-existance',
        help=check_todays_dated_folder_existance.__doc__)

    extract_todays_data = subparsers.add_parser(
        'extract-todays-data',
        help=extract_twitter_data.__doc__)

    extract_yesterdays_data = subparsers.add_parser(
        'extract-yesterdays-data',
        help=extract_twitter_data.__doc__)

    extract_data_from = subparsers.add_parser(
        'extract-data-from',
        help=extract_twitter_data.__doc__)

    extract_data_from.add_argument(
        'date',
        type=datetime.date.fromisoformat,
        help='Date format required: YYYY-MM-DD')

    #  start program

    args = parser.parse_args()
    warehouse_path = 'data/twitter_api/'

    if args.command == 'check-for-folder-existance':
        extract_data_from = args.date
        folder_exist = check_dated_folder_existance(warehouse_path, extract_data_from)
    elif args.command == 'check-for-todays-folder-existance':
        folder_exist = check_todays_dated_folder_existance(warehouse_path)
    elif args.command == 'check-for-yesterdays-folder-existance':
        folder_exist = check_yesterdays_dated_folder_existance(warehouse_path)
    elif args.command == 'extract-todays-data':
        extract_twitter_data(warehouse_path, extract_data_from='today')
    elif args.command == 'extract-yesterdays-data':
        extract_twitter_data(warehouse_path, extract_data_from='yesterday')
    elif args.command == 'extract-data-from':
        extract_twitter_data(warehouse_path, extract_data_from=args.date)
