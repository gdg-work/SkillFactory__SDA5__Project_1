#!/usr/bin/env python3
"""
A test program for working with PostgreSQL database via Python interface. Let's execute
a series of queries while doing most work automatically
"""
import psycopg2

# pseudo constants
MY_WEEKS = range(1,11)

def get_cohort_users_count(db_cursor: psycopg2.extensions.cursor) -> [int]:
    count_list = []
    for week_num in MY_WEEKS:
        db_cursor.execute("SELECT count(uid) from prj1.user where start_week={};".format(week_num))
        (ua,) = db_cursor.fetchone()
        count_list.append(ua)
    return count_list

def get_cohort_ap(db_cursor: psycopg2.extensions.cursor) -> [int]:
    sums_list = []
    for week_num in MY_WEEKS:
        db_cursor.execute("SELECT sum(cost) from prj1.user where start_week={};".format(week_num))
        (uap,) = db_cursor.fetchone()
        sums_list.append(uap)
    return sums_list

def do_work():
    try:
        db_conn = psycopg2.connect(user="dgolub", password="VunLurk5lam", host="172.17.0.2", port="5432", database="dgolub")
        cursor = db_conn.cursor()
        ua_by_wk = get_cohort_users_count(cursor)
        print("Visitors asquired by week cohort: \n\t" + ", ".join([str(i) for i in ua_by_wk]))
        cost_by_wk = get_cohort_ap(cursor)
        print("Cost of visitor asquirement by cohort:\n\t" + ", ".join(["{:g}".format(f) for f in cost_by_wk]))
        # Computing cost of user (CAU)
        print("Average cost per visitor:\n\t" +
                ", ".join(["{0:.2f}".format(f) for f in  map(lambda t: t[0]/t[1], zip(cost_by_wk, ua_by_wk))]))
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
