#!/usr/bin/env python3
"""
A test program for PostgreSQL database access
"""
import psycopg2

def do_work():
    try:
        db_conn = psycopg2.connect(user="dgolub", password="VunLurk5lam", host="172.17.0.2", port="5432", database="dgolub")
        cursor = db_conn.cursor()
        cursor.execute("SELECT * from prj1.log limit 10;")
        recs_list = cursor.fetchall()
        print("First 10 records in the database 'prj1.log' are:\n", "\n".join([str(r) for r in recs_list]))
    except (Exception, psycopg2.Error) as error :
        print ("Error while connecting to PostgreSQL", error)
    finally:
        if db_conn:
            cursor.close()
            db_conn.close()
            print("The database connection is closed")


if __name__ == "__main__":
    do_work()
    exit(0)
