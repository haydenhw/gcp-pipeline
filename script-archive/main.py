import json
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


def print_tweets(db):
    users_ref = db.collection('tweets')
    docs = users_ref.stream()

    for d in docs:
        print(d.to_dict())


def save_tweets(db):
    with open('../data/musk-tweets-1.txt') as f:
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


if __name__ == "__main__":
    db = firestore.Client()
    save_tweets(db)

