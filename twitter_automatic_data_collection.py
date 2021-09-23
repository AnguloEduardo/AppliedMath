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

    states = ['AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 'HI', 'ID', 'IL', 'IN',
              'IA', 'KS', 'KY', 'LA', 'ME', 'MD', 'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV',
              'NH', 'NJ', 'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN',
              'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY']
    # Alabama
    al_cities = ['33.516024,-86.812082,60mi',  # Birmingham
                 '32.375103,-86.306826,60mi',  # Montgomery
                 '34.726924,-86.584158,60mi',  # Huntsville
                 '30.693869,-88.041501,60mi',  # Mobile
                 '33.208951,-87.535222,60mi']  # Tuscaloosa
    # Alaska
    ak_cities = ['61.218571,-149.899714,20mi',  # Anchorage
                 '58.300895,-134.419082,60mi',  # Juneau
                 '64.836575,-147.713208,20mi',  # Fairbanks
                 '64.805799,-147.409994,20mi',  # Badger
                 '61.509919,-149.612032,00mi']  # Knik-Fairview
    # Arizona
    az_cities = ['33.458401,-112.074274,60mi',  # Phoenix
                 '32.200239,-110.910116,60mi',  # Tucson
                 '35.197515,-111.650054,60mi',  # Flagstaff
                 '35.233094,-114.012723,60mi',  # Kingman
                 '34.247880,-110.041879,60mi']  # Show Low
    # Arkansas
    ar_cities = ['34.742197,-92.296465,60mi',  # Little Rock
                 '36.067279,-94.152234,60mi',  # Fayetteville
                 '35.366890,-94.367198,60mi',  # Fort Smith
                 '33.577946,-92.836824,60mi',  # Camden
                 '35.838562,-90.702510,60mi']  # Jonesboro
    # California
    ca_cities = ['34.051527,-118.244841,60mi',  # Los Angeles
                 '32.727427,-117.138235,60mi',  # San Diego
                 '35.366041,-119.018075,60mi',  # Bakersfield
                 '37.797077,-122.407566,60mi',  # San Francisco
                 '36.777895,-119.789828,60mi']  # Fresno
    # Colorado
    co_cities = ['39.735948,-104.990777,60mi',  # Denver
                 '38.837389,-104.778573,60mi',  # Colorado Springs
                 '39.067066,-108.550315,60mi',  # Grand Junction
                 '40.555859,-105.079191,60mi',  # Fort Collins
                 '40.519568,-107.549102,60mi']  # Craig
    # Connecticut
    ct_cities = ['0,0,60mi',  # Bridgeport
                 '0,0,60mi',  # New Haven
                 '0,0,60mi',  # Stamford
                 '0,0,60mi',  # Hartford
                 '0,0,60mi']  # Waterbury
    de_cities = ['0,0,60mi',  # Wilmington
                 '0,0,60mi',  # Dover
                 '0,0,60mi',  # Newark
                 '0,0,60mi',  # Middletown
                 '0,0,60mi']  # Bear
    fl_cities = ['0,0,60mi',  # Jacksonville
                 '0,0,60mi',  # Miami
                 '0,0,60mi',  # Tampa
                 '0,0,60mi',  # Orlando
                 '0,0,60mi']  # St. Petersburg
    ga_cities = ['0,0,60mi',  # Atlanta
                 '0,0,60mi',  # Augusta
                 '0,0,60mi',  # Columbus
                 '0,0,60mi',  # Macon County
                 '0,0,60mi']  # Savannah
    hi_cities = ['0,0,60mi',  # Honolulu
                 '0,0,60mi',  # East Honolulu
                 '0,0,60mi',  # Pearl City
                 '0,0,60mi',  # Hilo
                 '0,0,60mi']  # Waipahu
    id_cities = ['0,0,60mi',  # Boise
                 '0,0,60mi',  # Meridian
                 '0,0,60mi',  # Nampa
                 '0,0,60mi',  # Idaho Falls
                 '0,0,60mi']  # Caldwell
    il_cities = ['0,0,60mi',  # Chicago
                 '0,0,60mi',  # Aurora
                 '0,0,60mi',  # Naperville
                 '0,0,60mi',  # Joliet
                 '0,0,60mi']  # Rockford
    in_cities = ['0,0,60mi',  # Indianapolis
                 '0,0,60mi',  # Fort Wayne
                 '0,0,60mi',  # Evansville
                 '0,0,60mi',  # Carmel
                 '0,0,60mi']  # South Bend
    ia_cities = ['0,0,60mi',  # Des Moines
                 '0,0,60mi',  # Cedar Rapids
                 '0,0,60mi',  # Davenport
                 '0,0,60mi',  # Sioux City
                 '0,0,60mi']  # Iowa City
    ks_cities = ['0,0,60mi',  # Wichita
                 '0,0,60mi',  # Overland Park
                 '0,0,60mi',  # Kansas City
                 '0,0,60mi',  # Olathe
                 '0,0,60mi']  # Topeka
    ky_cities = ['0,0,60mi',  # Louisville
                 '0,0,60mi',  # Lexington
                 '0,0,60mi',  # Bowling Green
                 '0,0,60mi',  # Owensboro
                 '0,0,60mi']  # Covington
    la_cities = ['0,0,60mi',  # New Orleans
                 '0,0,60mi',  # Baton Rouge
                 '0,0,60mi',  # Shreveport
                 '0,0,60mi',  # Metairie
                 '0,0,60mi']  # Lafayette
    me_cities = ['0,0,60mi',  # Portland
                 '0,0,60mi',  # Lewiston
                 '0,0,60mi',  # Bangor
                 '0,0,60mi',  # South Portland
                 '0,0,60mi']  # Auburn
    md_cities = ['0,0,60mi',  # Baltimore
                 '0,0,60mi',  # Columbia
                 '0,0,60mi',  # Germantown
                 '0,0,60mi',  # Silver Spring
                 '0,0,60mi']  # Waldorf
    ma_cities = ['0,0,60mi',  # Boston
                 '0,0,60mi',  # Worcester
                 '0,0,60mi',  # Springfield
                 '0,0,60mi',  # Cambridge
                 '0,0,60mi']  # Lowell
    mi_cities = ['0,0,60mi',  # Detroit
                 '0,0,60mi',  # Grand Rapids
                 '0,0,60mi',  # Warren
                 '0,0,60mi',  # Sterling Heights
                 '0,0,60mi']  # Lansing
    mn_cities = ['0,0,60mi',  # Minneapolis
                 '0,0,60mi',  # St. Paul
                 '0,0,60mi',  # Rochester
                 '0,0,60mi',  # Duluth
                 '0,0,60mi']  # Bloomington
    ms_cities = ['0,0,60mi',  # Jackson
                 '0,0,60mi',  # Gulfport
                 '0,0,60mi',  # Southaven
                 '0,0,60mi',  # Biloxi
                 '0,0,60mi']  # Hattiesburg
    mo_cities = ['0,0,60mi',  # Kansas City
                 '0,0,60mi',  # St. Louis
                 '0,0,60mi',  # Springfield
                 '0,0,60mi',  # Columbia
                 '0,0,60mi']  # Independence
    mt_cities = ['0,0,60mi',  # Billings
                 '0,0,60mi',  # Missoula
                 '0,0,60mi',  # Great Falls
                 '0,0,60mi',  # Bozeman
                 '0,0,60mi']  # Butte
    ne_cities = ['0,0,60mi',  # Omaha
                 '0,0,60mi',  # Lincoln
                 '0,0,60mi',  # Bellevue
                 '0,0,60mi',  # Grand Island
                 '0,0,60mi']  # Kearney
    nv_cities = ['0,0,60mi',  # Las Vegas
                 '0,0,60mi',  # Henderson
                 '0,0,60mi',  # Reno
                 '0,0,60mi',  # North Las Vegas
                 '0,0,60mi']  # Paradise
    nh_cities = ['0,0,60mi',  # Manchester
                 '0,0,60mi',  # Nashua
                 '0,0,60mi',  # Concord
                 '0,0,60mi',  # Dover
                 '0,0,60mi']  # Rochester
    nj_cities = ['0,0,60mi',  # Newark
                 '0,0,60mi',  # Jersey City
                 '0,0,60mi',  # Paterson
                 '0,0,60mi',  # Elizabeth
                 '0,0,60mi']  # Toms River
    nm_cities = ['0,0,60mi',  # Albuquerque
                 '0,0,60mi',  # Las Cruces
                 '0,0,60mi',  # Rio Rancho
                 '0,0,60mi',  # Santa Fe
                 '0,0,60mi']  # Roswell
    ny_cities = ['0,0,60mi',  # New York City
                 '0,0,60mi',  # Buffalo
                 '0,0,60mi',  # Rochester
                 '0,0,60mi',  # Yonkers
                 '0,0,60mi']  # Syracuse
    nc_cities = ['0,0,60mi',  # Charlotte
                 '0,0,60mi',  # Raleigh
                 '0,0,60mi',  # Greensboro
                 '0,0,60mi',  # Durham
                 '0,0,60mi']  # Winston-Salem
    nd_cities = ['0,0,60mi',  # Fargo
                 '0,0,60mi',  # Bismarck
                 '0,0,60mi',  # Grand Forks
                 '0,0,60mi',  # Minot
                 '0,0,60mi']  # West Fargo
    oh_cities = ['0,0,60mi',  # Columbus
                 '0,0,60mi',  # Cleveland
                 '0,0,60mi',  # Cincinnati
                 '0,0,60mi',  # Toledo
                 '0,0,60mi']  # Akron
    ok_cities = ['0,0,60mi',  # Oklahoma City
                 '0,0,60mi',  # Tulsa
                 '0,0,60mi',  # Norman
                 '0,0,60mi',  # Broken Arrow
                 '0,0,60mi']  # Edmond
    or_cities = ['0,0,60mi',  # Portland
                 '0,0,60mi',  # Salem
                 '0,0,60mi',  # Eugene
                 '0,0,60mi',  # Hillsboro
                 '0,0,60mi']  # Gresham
    pa_cities = ['0,0,60mi',  # Philadelphia
                 '0,0,60mi',  # Pittsburgh
                 '0,0,60mi',  # Allentown
                 '0,0,60mi',  # Erie
                 '0,0,60mi']  # Reading
    ri_cities = ['0,0,60mi',  # Providence
                 '0,0,60mi',  # Cranston
                 '0,0,60mi',  # Warwick
                 '0,0,60mi',  # Pawtucket
                 '0,0,60mi']  # East Providence
    sc_cities = ['0,0,60mi',  # Charleston
                 '0,0,60mi',  # Columbia
                 '0,0,60mi',  # North Charleston
                 '0,0,60mi',  # Mount Pleasant
                 '0,0,60mi']  # Rock Hill
    sd_cities = ['0,0,60mi',  # Sioux Falls
                 '0,0,60mi',  # Rapid City
                 '0,0,60mi',  # Aberdeen
                 '0,0,60mi',  # Brookings
                 '0,0,60mi']  # Watertown
    tn_cities = ['0,0,60mi',  # Nashville
                 '0,0,60mi',  # Knoxville
                 '0,0,60mi',  # Chattanooga
                 '0,0,60mi',  # Clarksville
                 '0,0,60mi']  # Murfreesboro
    tx_cities = ['0,0,60mi',  # Houston
                 '0,0,60mi',  # San Antonio
                 '0,0,60mi',  # Dallas
                 '0,0,60mi',  # Austin
                 '0,0,60mi']  # Fort Worth
    ut_cities = ['0,0,60mi',  # Salt Lake City
                 '0,0,60mi',  # West Valley City
                 '0,0,60mi',  # West Jordan
                 '0,0,60mi',  # Provo
                 '0,0,60mi']  # Orem
    vt_cities = ['0,0,60mi',  # Burlington
                 '0,0,60mi',  # South Burlington
                 '0,0,60mi',  # Rutland
                 '0,0,60mi',  # Essex Junction
                 '0,0,60mi']  # Bennington
    va_cities = ['0,0,60mi',  # Virginia Beach
                 '0,0,60mi',  # Chesapeake
                 '0,0,60mi',  # Norfolk
                 '0,0,60mi',  # Arlington
                 '0,0,60mi']  # Richmond
    wa_cities = ['0,0,60mi',  # Seattle
                 '0,0,60mi',  # Spokane
                 '0,0,60mi',  # Tacoma
                 '0,0,60mi',  # Vancouver
                 '0,0,60mi']  # Bellevue
    wv_cities = ['0,0,60mi',  # Charleston
                 '0,0,60mi',  # Huntington
                 '0,0,60mi',  # Morgantown
                 '0,0,60mi',  # Parkersburg
                 '0,0,60mi']  # Wheeling
    wi_cities = ['0,0,60mi',  # Milwaukee
                 '0,0,60mi',  # Madison
                 '0,0,60mi',  # Green Bay
                 '0,0,60mi',  # Kenosha
                 '0,0,60mi']  # Kenosha
    wy_cities = ['0,0,60mi',  # Cheyenne
                 '0,0,60mi',  # Casper
                 '0,0,60mi',  # Laramie
                 '0,0,60mi',  # Gillette
                 '0,0,60mi']  # Rock Springs
    all_cities = {'AL': al_cities, 'AK': ak_cities, 'AZ': az_cities, 'AR': ar_cities, 'CA': ca_cities, 'CO': co_cities,
                  'CT': ct_cities, 'DE': de_cities, 'FL': fl_cities, 'GA': ga_cities, 'HI': hi_cities, 'ID': id_cities,
                  'IL': il_cities, 'IN': in_cities, 'IA': ia_cities, 'KS': ks_cities, 'KY': ky_cities, 'LA': la_cities,
                  'ME': me_cities, 'MD': md_cities, 'MA': ma_cities, 'MI': mi_cities, 'MN': mn_cities, 'MS': ms_cities,
                  'MO': mo_cities, 'MT': mt_cities, 'NE': ne_cities, 'NV': nv_cities, 'NH': nh_cities, 'NJ': nj_cities,
                  'NM': nm_cities, 'NY': ny_cities, 'NC': nc_cities, 'ND': nd_cities, 'OH': oh_cities, 'OK': ok_cities,
                  'OR': or_cities, 'PA': pa_cities, 'RI': ri_cities, 'SC': sc_cities, 'SD': sd_cities, 'TN': tn_cities,
                  'TX': tx_cities, 'UT': ut_cities, 'VT': vt_cities, 'VA': va_cities, 'WA': wa_cities, 'WV': wv_cities,
                  'WI': wi_cities, 'WY': wy_cities}

    # Collect N Tweets with id, time, text, user data, likes, retweets and location (when available) FOR EACH OPERATOR
    for state in states:

        # Create empty lists
        tweet_id = []
        tweet_id_str = []
        tweet_time = []
        tweet_text = []
        user_id = []
        user_name = []
        user_screen_name = []
        likes = []
        retweets = []
        followers = []
        in_reply_to_screen_name = []
        in_reply_to_status_id_str = []
        location = []

        for index in range(5):
            cities = all_cities[state]
            coordinates = cities[index]

            tweets_processing(state, search_date, tweet_id, tweet_id_str, tweet_time, tweet_text, user_id,
                              user_name, user_screen_name, location, likes, retweets, followers,
                              in_reply_to_screen_name, in_reply_to_status_id_str, api, coordinates)

        csv_file(state, destination_folder, search_date, tweet_id, tweet_id_str, tweet_time, tweet_text, user_id,
                 user_name, user_screen_name, location, likes, retweets, followers, in_reply_to_screen_name,
                 in_reply_to_status_id_str)


def tweets_processing(operator, search_date, tweet_id, tweet_id_str, tweet_time, tweet_text, user_id,
                      user_name, user_screen_name, location, likes, retweets, followers, in_reply_to_screen_name,
                      in_reply_to_status_id_str, api, coordinates):
    import time as tm
    import tweepy
    from datetime import datetime, timedelta

    language = 'en'
    query = '#vaccine OR #COVID19 OR #NoVaccine OR #VaccineSideEffects'
    number_of_tweets = 1  # you decide the limit of tweets to extract per day
    until_date = str(search_date + timedelta(days=1))

    print('timestamp: ', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    print('Querying for ' + operator + ' on ' + str(search_date) + ' with ' + query)

    cursor = tweepy.Cursor(api.search, since_id=None, q=query, geocode=coordinates, lang=language,
                           until=until_date, tweet_mode='extended', wait_on_rate_limit=True). \
        items(number_of_tweets)

    while True:
        try:
            i = cursor.next()
            if i.created_at.date() == search_date:
                tweet_id.append(i.id)
                tweet_id_str.append(i.id_str)
                tweet_time.append(i.created_at)
                tweet_text.append(i.full_text)
                user_id.append(i.user.id)
                user_name.append(i.user.name)
                user_screen_name.append(i.user.screen_name)
                likes.append(i.favorite_count)
                retweets.append(i.retweet_count)
                followers.append(i.user.followers_count)
                in_reply_to_screen_name.append(i.in_reply_to_screen_name)
                in_reply_to_status_id_str.append(i.in_reply_to_status_id_str)

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
            print('PAUSED --> On ' + operator + ' collecting ' + str(len(tweet_id)) + ' tweets and CONTINUING')
            tm.sleep(60 * 15)
            continue
        except StopIteration:
            break


def csv_file(state, destination_folder, search_date, tweet_id, tweet_id_str, tweet_time, tweet_text, user_id,
             user_name, user_screen_name, location, likes, retweets, followers, in_reply_to_screen_name,
             in_reply_to_status_id_str):
    import pandas as pd
    from datetime import datetime

    # If len(list) > number_of_tweets --> no fue suficiente, hay que buscar de nuevo con otro limite.
    print('timestamp: ', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    print('For ' + state + ' collected ' + str(len(tweet_id)) + ' tweets')
    print()
    print()

    df = pd.DataFrame({
        'tweet_id': tweet_id,
        'tweet_id_str': tweet_id_str,
        'tweet_time': tweet_time,
        'tweet_text': tweet_text,
        'user_id': user_id,
        'user_name': user_name,
        'user_screen_name': user_screen_name,
        'location': location,
        'likes': likes,
        'retweets': retweets,
        'followers': followers,
        'in_reply_to_screen_name': in_reply_to_screen_name,
        'in_reply_to_status_id_str': in_reply_to_status_id_str,
    })

    #  Replace \n by space, to remove line jumps
    df = df.replace('\n', '', regex=True)

    #  Save DataFrame as .tsv
    df.to_csv(destination_folder + state + '_' + str(search_date).replace('-', '_') + '.tsv', index=False,
              encoding='utf_8_sig', sep='\t')


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
