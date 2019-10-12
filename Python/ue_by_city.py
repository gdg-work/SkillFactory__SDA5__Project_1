#!/usr/bin/env python3
"""
Расчёт ROMI, базируясь на данных из БД.
В этот раз нам нужен не когортный анализ, а подсчёт ROMI для текущего состояния проекта и разбивка его по двум
переменным: городу, где находится клиент, и способу привлечения клиента.

Города:  
    vladimir, ekb, spb, volgograd, orel, moscow, 
Способы привлечения:
    cpc_direct, cpc_adwords, smm, seo
(и то и другое получается из таблицы users простыми запросами)
"""
import psycopg2
import pandas as pd

# pseudo constants
MY_WEEKS = range(1,11)
DB_CONNECT_STRING="user=dgolub password=VunLurk5lam host=172.17.0.2 port=5432 dbname=dgolub"

""" Что есть ROMI? Это ARPU/CPA, где ARPU - "грязный" доход на пользователя, а
CPA - стоимость привлечения.  Стоимость привлечения пользователя легко
посчитать из таблиц log и user, зная стоимость привлечения пользователя и его
домашний регион.

ARPU = C1 * ARPC, где C1 конверсия (долей от 1) и ARPC - "грязный" доход на покупателя.

ARPC в нашем случае 100% маржи считается как AvP * APC, где AvP -- средний чек и APC --
среднее число покупок на покупателя, то есть T/B, где T-Transactions, B-Byers.
"""

# Шаблоны для запросов.  
# Это запрос в таблицу 'log' базы данных, не ссылающийся на таблицу 'user'
TMPL_NOPARAM = """
    select {0}
    from prj1.log
    where sum is not null;
    """
# Запрос к соединению таблиц log и user
TMPL_PARAM = """
    select {0}
    from prj1.log join prj1.user using (uid) 
    where sum is not null AND {1};
    """
def pandas_df_by_region_and_source(data_list) -> pd.DataFrame:
    """
    Формирует датафрейм из данных, которые переданы функции в качестве параметра.
    Формат данных: регион, способ привлечения, значение. 
    В получившемся датафрейме способы привлечения будут строками,
    регионы столбцами, значения -- ячейками.
    """
    newDF = pd.DataFrame()
    for (reg, src, data) in data_list:
        newDF.loc[src, reg] = data
    return newDF

def get_apc(db_cursor: "psycopg2.extensions.cursor, коннект в БД") -> tuple:
    """Возвращает среднее количество покупок на пользователя глобально и в
    разрезе регионов и способов привлечения.
    """
    AVERAGE_REQ = """select count(*)*1.0/count(distinct uid) as apc 
        from prj1.log where sum is not null"""

    MATRIX_REQ = """
        select 
            region,source,
            round(count(*)*1.0/count(distinct uid),2) as apc 
        from 
            prj1.log natural join prj1.user 
        where sum is not null 
        group by region,source;
        """

    # Среднее по всем регионам и методам привлечения
    db_cursor.execute(AVERAGE_REQ)
    (very_average_apc,) = db_cursor.fetchone()
    
    # В координатах "регион-метод":
    db_cursor.execute(MATRIX_REQ)
    df = pandas_df_by_region_and_source(db_cursor.fetchall())
    return (very_average_apc, df)


def print_db_data(db_cursor: psycopg2.extensions.cursor):
    (apc, apc_by_reg_src) = get_apc(db_cursor)
    print("Total APC: {:.2f}".format(apc))
    print(apc_by_reg_src)
    return

def do_work():
    try:
        db_conn = psycopg2.connect(DB_CONNECT_STRING)
        cursor = db_conn.cursor()
        print_db_data(cursor)
    except (Exception, psycopg2.Error) as error :
        print ("Error while working with PostgreSQL", error)
    finally:
        if db_conn:
            cursor.close()
            db_conn.close()
            print("The database connection is closed")

if __name__ == "__main__":
    do_work()
    exit(0)

#def get_cohort_users_count(db_cursor: psycopg2.extensions.cursor) -> [int]:
#    "Возвращает количество пользователей в когорте как список целых чисел"
#    count_list = []
#    for week_num in MY_WEEKS:
#        db_cursor.execute("SELECT count(uid) from prj1.user where start_week={};".format(week_num))
#        (ua,) = db_cursor.fetchone()
#        count_list.append(ua)
#    return count_list
#
#def get_cohort_ap(db_cursor: psycopg2.extensions.cursor) -> [float]:
#    "возвращает стоимость привлечения пользователей как список вещественных чисел"
#    sums_list = []
#    for week_num in MY_WEEKS:
#        db_cursor.execute("SELECT sum(cost) from prj1.user where start_week={};".format(week_num))
#        (uap,) = db_cursor.fetchone()
#        sums_list.append(uap)
#    return sums_list
#
#def request_by_cohort_and_week(db_cursor: psycopg2.extensions.cursor, req: str) -> []:
#    """Выполняет запрос в базу, возвращает результат как список строк, где каждая строка -- кортеж полей"""
#    db_cursor.execute(req)
#    return db_cursor.fetchall()
#
#
#def pandas_df_by_cohort_and_week(data_list) -> pd.DataFrame:
#    """
#    Формирует датафрейм из данных, которые переданы функции в качестве параметра.
#    Формат данных: когорта, неделя, значение. В получившемся датафрейме когорты будут строками,
#    недели столбцами, значения, естественно, ячейками.
#    """
#    newDF = pd.DataFrame()
#    for (cohort, week, data) in data_list:
#        newDF.loc[cohort, week] = data
#    return newDF
#
#COHORT_WEEK_QUERY_TEMPLATE="""select u.start_week, l.week, {0}
#    from prj1.log as l natural join prj1.user as u
#    where u.start_week is not null and
#    {1}
#    group by u.start_week, l.week
#    order by u.start_week, l.week;"""
#
#def get_users_by_cohort_week(db_cursor: psycopg2.extensions.cursor) -> pd.DataFrame:
#    """
#    Возвращает количество ПОСЕТИТЕЛЕЙ с разбивкой по когортам и неделям в виде Pandas Dataframe
#    """
#    req = COHORT_WEEK_QUERY_TEMPLATE.format("count(distinct l.uid)", "l.sum is null")
#    print("Request to DB: {}\n".format(req))
#    print("== USERS ==")
#    return pandas_df_by_cohort_and_week(request_by_cohort_and_week(db_cursor, req))
#
#def get_buyers_by_cohort_week(db_cursor: psycopg2.extensions.cursor) -> pd.DataFrame:
#    """
#    Возвращает количество ПОКУПАТЕЛЕЙ с разбивкой по когортам и неделям в виде Pandas Dataframe
#    """
#    req = """select u.start_week, b.fpweek, count(distinct u.uid)
#        from prj1.log as l left join prj1.user as u on l.uid = u.uid left join prj1.first_buy as b on l.uid = b.uid
#        where sum is not null
#        group by u.start_week, b.fpweek
#        order by u.start_week, b.fpweek;"""
#    print("Request to DB: {}\n".format(req))
#    print("== BUYERS ==")
#    return pandas_df_by_cohort_and_week(request_by_cohort_and_week(db_cursor, req))
#
#def get_transactions_by_cohort_week_old(db_cursor: psycopg2.extensions.cursor) -> pd.DataFrame:
#    """
#    Возвращает количество транзакций, совершённых участниками когорт с разбивкой по когортам и неделям
#    """
#    req = COHORT_WEEK_QUERY_TEMPLATE.format("count(*)", "l.sum is not null and l.sum > 0")
#    print("Request to DB: {}\n".format(req))
#    print("== TRANSACTIONS ==")
#    return pandas_df_by_cohort_and_week(request_by_cohort_and_week(db_cursor, req))
#
#def get_transactions_by_cohort_week(db_cursor: psycopg2.extensions.cursor) -> pd.DataFrame:
#    """
#    Возвращает количество транзакций, совершённых участниками когорт с разбивкой по когортам и неделям
#    """
#    req = """
#    select 
#        start_week, fpweek, count(uid) 
#    from 
#        prj1.log join prj1.user using(uid) 
#                 join prj1.first_buy using (uid) 
#    where sum is not null 
#    group by start_week, fpweek 
#    order by start_week, fpweek;
#    """
#    print("Request to DB: {}\n".format(req))
#    print("== TRANSACTIONS ==")
#    return pandas_df_by_cohort_and_week(request_by_cohort_and_week(db_cursor, req))
#
#def get_apc_by_cohort_week(db_cursor: psycopg2.extensions.cursor) -> pd.DataFrame:
#    """Возвращает APC за неделю по когортам
#    APC считаетя, как 1/(число пользователей когорты, которые впервые совершили покупку
#    в данную неделю) * число транзакций, совершённых этими пользователями __за всё время__."""
#    req = """
#        select 
#            u.start_week, 
#            l.week, 
#            count(uid)*1.0/count(distinct uid) as apc
#        from 
#            prj1.log as l 
#            join prj1.user as u using (uid)
#            join prj1.first_buy as fb using (uid)
#        where 
#            u.start_week is not null 
#            and
#            l.sum is not null 
#            and 
#            l.sum > 0
#            and
#            l.week = fb.fpweek
#        group by u.start_week, l.week
#        order by u.start_week, l.week;
#    """
#    print("Request to DB: {}\n".format(req))
#    print("== APC ==")
#    return pandas_df_by_cohort_and_week(request_by_cohort_and_week(db_cursor, req))
#
#def get_gross_profit_by_cohort_week(db_cursor: psycopg2.extensions.cursor) -> pd.DataFrame:
#    """
#    Возвращает суммарную стоимость покупок с разбивкой по когортам и неделям
#    """
#    req = COHORT_WEEK_QUERY_TEMPLATE.format("sum(sum)", "l.sum is not null and l.sum > 0")
#    print("Request to DB: {}\n".format(req))
#    print("== TRANSACTIONS ==")
#    return pandas_df_by_cohort_and_week(request_by_cohort_and_week(db_cursor, req))
#
