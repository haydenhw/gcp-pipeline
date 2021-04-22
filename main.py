import os
import json
import re

import mysql.connector
from google.cloud import firestore

# set GOOGLE_APPLICATION_CREDENTIALS=\path\to\credentials.json
e = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
if not e:
    print("WARN: Google Credentials Not Set")


def delete_tweets(batch_size=300):
    coll_ref = db.collection('tweets')
    docs = coll_ref.limit(batch_size).stream()
    deleted = 0

    for doc in docs:
        print(f'Deleting doc {doc.id} => {doc.to_dict()}')
        doc.reference.delete()
        deleted = deleted + 1

    if deleted >= batch_size:
        return delete_tweets(batch_size)


def add_row(db):
    doc_ref = db.collection('users').document('adama')
    doc_ref.set({
        'first': 'adam',
        'last': 'amler'
    })


def read_rows(db):
    users_ref = db.collection('users')
    docs = users_ref.stream()

    for d in docs:
        print(d.to_dict())


def save_tweets(db):
    with open('musk-tweets-1.txt') as f:
        print('Saving tweets...')
        for line in f:
            tweet = json.loads(line)
            doc_ref = db.collection('tweets').document(tweet['id'])
            doc_ref.set(tweet)


def print_tweets(db):
    tweets_ref = db.collection('tweets')
    docs = tweets_ref.stream()

    print('Fetching tweets...')
    for d in docs:
        print(d.to_dict())


def deEmojify(text):
    regrex_pattern = re.compile(pattern = "["
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
        tweet_id = d['id']
        retweet_count = d['public_metrics']['retweet_count']
        created_at = d['created_at']
        text = deEmojify(d['text'])
        print("Saving:")
        print(text, created_at, retweet_count)

        try:
            cur.execute("""insert into tweets values(%s, %s, %s)""", (text, created_at, retweet_count))
            con.commit()
        except:
            print("Couldn't load the following row:")
            print(text, created_at, retweet_count)



def fetch_sql_rows(cur):
    cur.execute("select * from tweets")
    res = cur.fetchall()
    for row in res:
        print(row)


def get_mysql_con():
    return mysql.connector.connect(
        host='34.72.210.141',
        user='root',
        database='cloud',
        password=os.getenv('MYSQL_PW')
    )


def create_tweet_table(con, cur):
    cur.execute("create table tweets(text text, created_at text, retweet_count int)")
    con.commit()


def truncate_tweets(con, cur):
    cur.execute("truncate table tweets")
    con.commit()

if __name__ == "__main__":
    db = firestore.Client()
    con = get_mysql_con()
    cur = con.cursor()


    fetch_sql_rows(cur)

