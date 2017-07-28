import csv
import tweepy
import sqlite3 as sqlite
from collections import defaultdict, OrderedDict
from textblob.sentiments import NaiveBayesAnalyzer
from textblob import Blobber
tb = Blobber(analyzer=NaiveBayesAnalyzer())
import pandas as pd
import os

YOUR_UNIQNAME = 'mithils'  # Fill in your uniqname
DB_PATH = "si330-final-project.db".format(YOUR_UNIQNAME)

# Unique code from Twitter
access_token = "791818305707335681-dclJMCNmxy8o2AewAYF151lezepQVsR"
access_token_secret = "JqIAWuCOKdw5b3FMzOkWzY2A07BAbNBonuyFATrMIH8UQ"
consumer_key = "3NuHlgUd7V6bh4luLzufApZyd"
consumer_secret = "mne0VBThE0LhQILiTaMH4mdK8LMxW0ChVhEUEZQdE23ZYFGob9"

# Boilerplate code here
auth = tweepy.AppAuthHandler(consumer_key, consumer_secret)

api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)


#
# Approach:
# 1) Get list of top 50 movie data
# 2) Use list to compile list of movie names
# 3) Use list of movie names in twitter search api
# 4) Save tweets of each movie as csv
# 5) Run sentiment analysis on each tweet
# 7) Rank movies using step 5
# 8) Create visualizations and comparisons
# 9) Create video

# Input: Reads in movie_data.csv
# This function reads in data from movie_data.csv to get movie name and movie imdb score
# These attributes are saved in a tuple which is appended to a list(movie_list)
# Output: movie_list (list)
def get_top_movies():
    movie_list = []
    with open('movie_data.csv', 'r', newline='') as input_file:
        country_data_reader = csv.DictReader(input_file, delimiter=',', quotechar='"')
        for row in country_data_reader:
            if len(row['director_name']) > 0 and int(row['num_voted_users']) >= 10000:
                movie_n = row['movie_title']
                movie_name = movie_n.replace(u'\xa0', u'')
                movie_imdb = row['imdb_score']
                movie_tup = (movie_name, movie_imdb)
                movie_list.append(movie_tup)
        return movie_list

# Input: None
# This function creates the db table used later for sql queries
def create_db_table():
    with sqlite.connect(DB_PATH) as conn:
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS  movie")
        cur.execute('CREATE TABLE  movie(movie_name TEXT, imdb_rating REAL)')
        cur.executemany("INSERT INTO movie VALUES(?, ?)", get_top_movies())
        conn.commit()
# Input: si330-final-project.db
# This function runs a sql query on the input db. Returns an ordered dict of top 50 IMDb movies based on IMDb ratings
# Output: result_set (orderedDict)
def sql_query():
    def dict_factory(cursor, row):
        d = OrderedDict([(x[0], None) for x in cursor.description])
        for idx, col in enumerate(cursor.description):
            d[col[0]] = row[idx]
        return d
    with sqlite.connect(DB_PATH) as conn:
        conn.row_factory = dict_factory
        cur = conn.cursor()
        cur.execute("SELECT movie_name, imdb_rating from movie ORDER By imdb_rating DESC LIMIT 50")
        result_set = cur.fetchall()
    return result_set

# Input: filename: filename for output txt file which was in the format of [movie name] tweets.txt
#        search_query: variable created from compile_ratings() which specifies movie name
# This function gets the tweets associated with each movie
# Output: Tweets for each movie in individual text files. Each tweet is separated by a newline

def get_tweets(filename, search_query):
    since_Id = None
    max_id = -1
    tweets_max = 3000
    query_tweets = 100
    t_count = 0
    tweets_dict = {}
    with open(filename, 'w',newline='',encoding='utf-8') as f:
        while t_count < tweets_max:
            try:
                if max_id <= 0:
                    if not since_Id:
                        new_tweets = api.search(q=search_query, count=query_tweets, lang = 'en')
                    else:
                        new_tweets = api.search(q=search_query, count=query_tweets,lang = 'en',
                                                since_id=since_Id)

                else:
                    if not since_Id:
                        new_tweets = api.search(q=search_query, count=query_tweets, lang = 'en',
                                                max_id=str(max_id - 1))

                    else:
                        new_tweets = api.search(q=search_query, count=query_tweets, lang = 'en',
                                                max_id=str(max_id - 1),
                                                since_id=since_Id)

                if not new_tweets:
                    print("No more tweets found")
                    break
            except tweepy.TweepError as e:
            # Just exit if any error
                print("some error : " + str(e))
                break
            t_count += len(new_tweets)
            print (tweets_dict)
            print("Downloaded {0} tweets".format(t_count))
            max_id = new_tweets[-1].id
            for tweet in new_tweets:
                f.write(tweet.text+"\n")

# Input: filename: filename of movie tweets text files
# This function was used to read every movie text file created by get_tweets()
# then runs a sentiment analysis using Naive Bayes Analyzer and calculate Twitter
# rating for each movie
# Output: movie_rating (int) : Movie rating associated with movie tweet file
def tweet_analysis(filename):
    file = open(filename, 'r').read().splitlines()
    pos_tweet = set()
    neg_tweet = set()
    for line in file:
        a = line
        analysis = tb(a)
        if analysis.sentiment.p_pos > .5:
            pos_tweet.add(a)
        elif analysis.sentiment.p_neg > .5:
            neg_tweet.add(a)
        else:
            continue
    positive_tweets = list(pos_tweet)
    negative_tweets = list(neg_tweet)
    num_tweets = len(positive_tweets) + len(negative_tweets)
    print (filename)
    print (num_tweets)
    movie_rating = len(positive_tweets)/num_tweets
    print (movie_rating)
    return movie_rating

# Input: Movie data from sql_query()(orderedDict)
# This function combines the list of top 50 movies with their respective Twitter Movie Ratings
# It also serves as the central function of this project as it utilizes multiple functions as
# well as os.path.exists to check if files already exist.
# Output: ratings_dict(dictionary) key: filename, value: Twitter Movie Rating

def compile_ratings():
    movie_data = sql_query()
    ratings_dict = {}
    for movie in movie_data:
        search_query = movie['movie_name']
        file_n = movie['movie_name']
        filename = ('{}_tweets.txt'.format(file_n))
        if os.path.exists(filename):
            ratings = tweet_analysis(filename)
            ratings_dict[file_n] = ratings
        else:
            get_tweets(filename, search_query)
            ratings = tweet_analysis(filename)
            ratings_dict[file_n] = ratings
    #print(ratings_dict.keys())
    return ratings_dict

# Input: ratings_dict from compile_ratings()
# This function creates the twitter_rank.csv and imdb_rank.csv files for data visualization
# Output: twitter_rank.csv & imdb_rank.csv with columns (Movie Name & IMDB/Twitter Rating)


def create_graph():
    filename = 'twitter_rank.csv'
    ratings_table = []
    ratings_dict = compile_ratings()
    for k,v in ratings_dict.items():
        ratings_table.append(({
            'Movie Name': k, 'Twitter Rating': v
        }))
    df_1 = pd.DataFrame(ratings_table)
    sort_1 = df_1.sort_values(by=['Twitter Rating'], ascending=False)
    sort_1.to_csv(filename, index=False, encoding='utf-8')
    filename_2 = 'imdb_rank.csv'
    imdb_table = []
    imdb_data = sql_query()
    imdb_dict = {}
    for x in imdb_data:
        movie = x['movie_name']
        imdb_rating = x['imdb_rating']
        imdb_dict[movie] = imdb_rating
    for k,v in imdb_dict.items():
        imdb_table.append(({
            'Movie Name': k, 'IMDb Rating': v
        }))
    df_2 = pd.DataFrame(imdb_table)
    sort_2 = df_2.sort_values(by=['IMDb Rating'], ascending=False)
    sort_2.to_csv(filename_2, index=False, encoding ='utf-8')







if __name__ == '__main__':
    create_graph()
