#!/usr/bin/env python3
"""
A test program for working with PostgreSQL database via Python interface. Let's execute
a series of queries while doing most work automatically
"""
import psycopg2
import pandas as pd

# pseudo constants
MY_WEEKS = range(1,11)
# -- for home machine DB_CONNECT_STRING="user=dgolub, password=VunLurk5lam, host=172.17.0.2, port=5432, dbname=dgolub"
DB_CONNECT_STRING="user=dgolub dbname=dgolub"

def get_cohort_users_count(db_cursor: psycopg2.extensions.cursor) -> [int]:
    count_list = []
    for week_num in MY_WEEKS:
        db_cursor.execute("SELECT count(uid) from prj1.user where start_week={};".format(week_num))
        (ua,) = db_cursor.fetchone()
        count_list.append(ua)
    return count_list

def get_cohort_ap(db_cursor: psycopg2.extensions.cursor) -> [float]:
    sums_list = []
    for week_num in MY_WEEKS:
        db_cursor.execute("SELECT sum(cost) from prj1.user where start_week={};".format(week_num))
        (uap,) = db_cursor.fetchone()
        sums_list.append(uap)
    return sums_list

def request_by_cohort_and_week(db_cursor: psycopg2.extensions.cursor, req: str) -> []:
    """Выполняет запрос в базу, возвращает результат как список строк, где каждая строка есть кортеж полей"""
    db_cursor.execute(req)
    return db_cursor.fetchall()


def pandas_df_by_cohort_and_week(data_list) -> pd.DataFrame:
    """
    Формирует датафрейм из данных, которые переданы функции в качестве параметра.
    Формат данных: когорта, неделя, значение. В получившемся датафрейме когорты будут строками,
    недели столбцами, значения, естественно, ячейками.
    """
    newDF = pd.DataFrame()
    for (cohort, week, data) in data_list:
        newDF.loc[cohort, week] = data
    return newDF

COHORT_WEEK_QUERY_TEMPLATE="""select u.start_week, l.week, {0}
    from prj1.log as l natural join prj1.user as u
    where u.start_week is not null and
    {1}
    group by u.start_week, l.week
    order by u.start_week, l.week;"""

def get_users_by_cohort_week(db_cursor: psycopg2.extensions.cursor) -> pd.DataFrame:
    """
    Возвращает количество покупателей с разбивкой по когортам и неделям в виде Pandas Dataframe
    """
    req = COHORT_WEEK_QUERY_TEMPLATE.format("count(distinct l.uid)", "l.sum is null")
    print("Request to DB: {}\n".format(req))
    print("== USERS ==")
    return pandas_df_by_cohort_and_week(request_by_cohort_and_week(db_cursor, req))

def get_buyers_by_cohort_week(db_cursor: psycopg2.extensions.cursor) -> pd.DataFrame:
    """
    Возвращает количество покупателей с разбивкой по когортам и неделям в виде Pandas Dataframe
    """
    req = """select u.start_week, b.fpweek, count(distinct u.uid)
        from prj1.log as l left join prj1.user as u on l.uid = u.uid left join prj1.first_buy as b on l.uid = b.uid
        where sum is not null
        group by u.start_week, b.fpweek
        order by u.start_week, b.fpweek;"""
    print("Request to DB: {}\n".format(req))
    print("== BUYERS ==")
    return pandas_df_by_cohort_and_week(request_by_cohort_and_week(db_cursor, req))

def get_transactions_by_cohort_week(db_cursor: psycopg2.extensions.cursor) -> pd.DataFrame:
    """
    Возвращает количество транзакций, совершённых участниками когорт с разбивкой по когортам и неделям
    """
    req = COHORT_WEEK_QUERY_TEMPLATE.format("count(*)", "l.sum is not null and l.sum > 0")
    print("Request to DB: {}\n".format(req))
    print("== TRANSACTIONS ==")
    return pandas_df_by_cohort_and_week(request_by_cohort_and_week(db_cursor, req))

def get_gross_profit_by_cohort_week(db_cursor: psycopg2.extensions.cursor) -> pd.DataFrame:
    """
    Возвращает суммарную стоимость покупок с разбивкой по когортам и неделям
    """
    req = COHORT_WEEK_QUERY_TEMPLATE.format("sum(sum)", "l.sum is not null and l.sum > 0")
    print("Request to DB: {}\n".format(req))
    print("== TRANSACTIONS ==")
    return pandas_df_by_cohort_and_week(request_by_cohort_and_week(db_cursor, req))

def print_db_data(db_cursor: psycopg2.extensions.cursor):
    ua_by_wk = get_cohort_users_count(db_cursor)
    print("Visitors asquired by week cohort: \n\t" + ", ".join([str(i) for i in ua_by_wk]))
    cost_by_wk = get_cohort_ap(db_cursor)
    print("Cost of visitor asquirement by cohort:\n\t" + ", ".join(["{:g}".format(f) for f in cost_by_wk]))
    # Computing cost of user (UAC)
    print("Average cost per visitor:\n\t" +
            ", ".join(["{0:.2f}".format(cost/count) for (cost, count) in zip(cost_by_wk, ua_by_wk)]))
    print("Attempt to fill Pandas DF")
    print(get_users_by_cohort_week(db_cursor))
    print(get_buyers_by_cohort_week(db_cursor))
    print(get_buyers_by_cohort_week(db_cursor).to_csv())
    print(get_transactions_by_cohort_week(db_cursor))
    print(get_gross_profit_by_cohort_week(db_cursor))
    return

def do_work():
    try:
        db_conn = psycopg2.connect(DB_CONNECT_STRING)
        cursor = db_conn.cursor()
        print_db_data(cursor)
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
