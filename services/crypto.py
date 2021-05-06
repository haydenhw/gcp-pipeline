import os
import json
import requests
import datetime
from util.db import get_connection


def fetch_eth_last_60min():
    api_key = os.getenv("CC_API_KEY")
    url = f"https://min-api.cryptocompare.com/data/v2/histominute?fsym=ETH&tsym=USD&limit=60&api_key={api_key}"
    print("Fetching ethereum history...")
    r = requests.get(url)
    j = r.json()
    history = j['Data']['Data']
    rows = [
        dict(
            close=row['close'],
            ts=row['time'],
            dt=datetime.datetime.fromtimestamp(row['time']).strftime('%Y-%m-%d %H:%M:%S')
        ) for row in history
    ]
    return rows


def create_table(con):
    cur = con.cursor()
    cur.execute("create table crypto_hist (price float, ts int, dt text)")
    con.commit()


def insert_row(con):
    cur = con.cursor()
    cur.execute("delete from crypto_hist where price = 123.5")
    con.commit()


def fetch_rows(con):
    cur = con.cursor()
    cur.execute("select * from crypto_hist")
    rows = cur.fetchall()

    for r in rows:
        print(r)

def count_rows(con):
    cur = con.cursor()
    cur.execute("select count(*) from crypto_hist")
    count = cur.fetchone()[0]
    print(f"Rows in table crypto_hist: {count}")


def save_hist(con, hist):
    cur = con.cursor()
    rows = [
        (
            row['close'],
            row['ts'],
            row['dt']
        ) for row in hist
    ]

    args_str = ",".join(cur.mogrify("(%s, %s, %s)", r).decode("utf_8") for r in rows)
    print(f"Saving {len(rows)} rows")
    cur.execute(f"insert into crypto_hist values {args_str}")
    con.commit()


def truncate_hist(con):
    cur = con.cursor()
    cur.execute("truncate table crypto_hist")
    con.commit()


def update_eth_hist(pg_con):
    eth_hist = fetch_eth_last_60min()
    save_hist(pg_con, eth_hist)
    count_rows(pg_con)


if __name__ == "__main__":
    pg_con = get_connection()
    update_eth_hist(pg_con)
