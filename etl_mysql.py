import mysql.connector
from google.cloud import firestore
import os
import re


def remove_emojis(text):
    regrex_pattern = re.compile(pattern="["
                                        u"\U0001F600-\U0001F64F"  # emoticons
                                        u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                                        u"\U0001F680-\U0001F6FF"  # transport & map symbols
                                        u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                                        "]+", flags=re.UNICODE)
    return regrex_pattern.sub(r'', text)


def load_tweets(db, con, cur):
    tweets_ref = db.collection('tweets')
    docs = tweets_ref.stream()

    for doc in docs:
        d = doc.to_dict()
        retweet_count = d['public_metrics']['retweet_count']
        created_at = d['created_at']
        text = remove_emojis(d['text'])
        print("Saving:")
        print(text, created_at, retweet_count)

        try:
            cur.execute("""insert into tweets values(%s, %s, %s)""", (text, created_at, retweet_count))
            con.commit()
        except:
            print("Couldn't load the following row:")
            print(text, created_at, retweet_count)


def create_tweet_table(con, cur):
    cur.execute("create table if not exists tweets(text text, created_at text, retweet_count int)")
    con.commit()


def truncate_tweet_table(con, cur):
    cur.execute("truncate table tweets")
    con.commit()


def get_mysql_con():
    return mysql.connector.connect(
        host='34.72.210.141',
        user='root',
        database='cloud',
        password=os.getenv('MYSQL_PW')
    )


def print_mysql_tweets(cur):
    cur.execute("select * from tweets")
    res = cur.fetchall()
    for row in res:
        print(row)


if __name__ == "__main__":
    db = firestore.Client()
    con = get_mysql_con()
    cur = con.cursor()

    load_tweets()
