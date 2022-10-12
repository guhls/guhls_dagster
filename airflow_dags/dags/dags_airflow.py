import sqlite3
import os
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine


def create_db():
    path_root = Path(__file__).parent.parent.parent
    db_name = "database_raw.sqlite3"

    if os.path.exists(path_root / db_name):
        os.remove(path_root / db_name)

    conn = sqlite3.connect(path_root / db_name)
    cur = conn.cursor()

    query = """CREATE TABLE client(id_client INTEGER PRIMARY KEY,name VARCHAR,email VARCHAR UNIQUE)"""
    cur.execute(query)

    client_list = [
        ("Gustavo", "gustavo@email.com"),
        ("Alex", "alex@email.com"),
        ("Luciana", "luciana@email.com"),
        ("Bia", "bia@email.com"),
    ]

    cur.executemany("INSERT INTO client(name, email) VALUES(?, ?)", client_list)

    return conn


def extract_from_db(table_name, conn):
    query = f"SELECT * FROM {table_name}"
    return pd.read_sql_query(query, conn)


def transform_split_email_field(df):
    df_split = df.email.str.split("@", expand=True)

    return df.assign(
        username=df_split[0],
        domain=df_split[1],
    )


def load_df_to_dwh(df_final):
    engine = create_engine("sqlite:///db_analytics.sqlite3", echo=False)
    df_final.to_sql("client_with_username", con=engine, if_exists="replace")


def etl_pipe():
    conn = create_db()
    df = extract_from_db(table_name="client", conn=conn)
    df_final = transform_split_email_field(df)
    load_df_to_dwh(df_final)


if __name__ == '__main__':
    # df = pd.DataFrame(
    #     {
    #         "id_client": [1, 2, 3, 4],
    #         "name": ["Gustavo", "Alex", "Luciana", "Bia"],
    #         "email": ["gustavo@email.com", "alex@email.com", "luciana@email.com", "bia@email.com"],
    #     }
    # )
    ...

