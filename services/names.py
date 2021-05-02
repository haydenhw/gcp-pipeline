import os
import psycopg2


def get_connection():
    return psycopg2.connect(
        database="mig",
        user="postgres",
        password=os.getenv("POSTGRES_PW"),
        host="35.196.215.29",
        port="5432"
    )


def print_names(con):
    cur = con.cursor()
    cur.execute("select * from names")
    rows = cur.fetchall()

    for r in rows:
        print(r)

def list_names_by_year(con, year):
    cur = con.cursor()
    cur.execute("select * from names where year=%s", (year,))
    return cur.fetchall()


def count_names(con):
    cur = con.cursor()
    cur.execute("select count(*) from names")
    count = cur.fetchone()
    print(f"Num rows: {count[0]}")


def save_names_from_disk(con):
    cur = con.cursor()
    names_dir = "../data/names"
    for f in os.listdir(names_dir)[:-1]:
        path = os.path.join(names_dir, f)
        print(f"Saving rows at path '{path}'")
        with open(path) as f:
            rows = f.readlines()
            rows = [r.split(",") for r in rows]
            rows = [
                (
                    r[0],
                    r[1],
                    int(r[2]),
                    int(r[3].rstrip("\n")),
                ) for r in rows
            ]
            args_str = ",".join(cur.mogrify("(%s, %s, %s, %s)", r).decode("utf_8") for r in rows)
            cur.execute(f"insert into names values {args_str}")

    con.commit()


def truncate_names(con):
    cur = con.cursor()
    cur.execute("truncate table names")
    con.commit()


if __name__ == "__main__":
    con = get_connection()
    names = list_names_by_year(con, 1990)



