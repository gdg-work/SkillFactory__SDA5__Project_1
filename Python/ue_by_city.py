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

METRIC_NAMES = ('apc', 'avp', 'ua', 'cpa', 'c1%', 'arpc', 'arpu', 'romi%')

# SQL queries templates

## Получение глобальных параметров юнит-экономики для всей БД.
GLOBAL_PARAMS = """
    with 
        apc as (
            select count(*)*1.0/count(distinct uid) as apc from prj1.log where sum is not null
        ),
        avp as (
            select sum(l.sum)/count(*) as avp from prj1.log as l where l.sum is not null
        ),
        ua as (
            select count(distinct uid) as ua from prj1.log
        ),
        cpa as (
            -- пользователей, имеющихся в таблице user, но не заходивших на сайт, 
            -- отрезаем с помощью поля first_visit
            select sum(cost)/count(distinct uid) as cpa from prj1.user where first_visit is not null
        ),
        c1 as (
            select count(distinct p.uid)*1.0/count(distinct l.uid) as c1
            from prj1.log as l left join prj1.first_buy as p using (uid)
        ),
        db_metrics as (
            select apc, avp, ua, cpa, c1
            from apc cross join avp cross join ua cross join cpa cross join c1
        )
        select 
            apc, avp, ua, cpa,
            c1   * 100        as "c1 %",
            avp  * apc        as arpc,
            avp  * apc * c1   as arpu,
            (avp * apc * c1 * 100)/cpa  as "romi %"
        from db_metrics;

    """

## Получение параметров юнит-экономики параметризованным запросом: первый параметр функции 'format'
## может быть 'source', 'region' или их комбинация: 'source, region' или 'region, source'
METRICS_BY_PARAM_TMPL = """
    with 
        apc as (
            select 
                {0},count(*)*1.0/count(distinct uid) as apc 
            from prj1.log natural join prj1.user 
            where sum is not null
            group by {0}
        ),
        avp as (
            select {0}, sum(l.sum)/count(*) as avp 
            from prj1.log as l natural join prj1.user
            where l.sum is not null
            group by {0}
        ),
        ua as (
            select {0},count(distinct uid) as ua 
            from prj1.log natural join prj1.user
            group by {0}
        ),
        cpa as (
            -- пользователей, имеющихся в таблице user, но не заходивших на сайт, 
            -- отрезаем с помощью поля first_visit
            select {0}, sum(cost)/count(distinct uid) as cpa 
            from prj1.user 
            where first_visit is not null
            group by {0}
        ),
        c1 as (
            select 
                {0}, count(distinct p.uid)*1.0/count(distinct l.uid) as c1
            from 
                prj1.log as l 
                left join prj1.first_buy as p using (uid) 
                join prj1.user using (uid)
            group by {0}
        ),
        db_metrics as (
            select {0}, apc, avp, ua, cpa, c1
            from 
                apc 
                join avp using ({0})
                join ua  using ({0})
                join cpa using ({0})
                join c1  using ({0})
        )
        select 
            {0}, apc, avp, ua, cpa,
            c1   * 100        as "c1 %",
            avp  * apc        as arpc,
            avp  * apc * c1   as arpu,
            (avp * apc * c1 * 100)/cpa  as "romi %"
        from db_metrics
        order by {0};
"""

##
## Все get_ - функции получают первым параметром объект класса psycopg2.extensions.cursor,
## который обеспечивает связь с БД.  После выполнения запроса методом '.execute' этого
## класса, необходимо забрать результаты методом '.fetchone' (возвращается кортеж полей)
## или методом '.fetchall' (возвращается список кортежей)

def get_globals(db_cursor) -> pd.DataFrame:
    """Возвращает глобальные параметры юнит-экономики: все регионы, 
    все методы привлечения"""
    temp_dict = {}
    db_cursor.execute(GLOBAL_PARAMS)
    (temp_dict['apc'], temp_dict['avp'], temp_dict['ua'], temp_dict['cpa'],
        temp_dict['c1%'], temp_dict['arpc'], temp_dict['arpu'],
        temp_dict['romi%']) = db_cursor.fetchone()
    df = pd.DataFrame(temp_dict, index=['Totals'])
    return df

def check_ue_grouping(grouping) -> (bool, str):
    """Проверяет строку на то, что это корректные параметры для группировки.
    grouping может быть None, строка или кортеж.
    Возвращает всегда кортеж из 2 значений:
    1) Булевое, корректна ли группировка
    2) строка для группировки (lower-case, если два параметра - они через запятую).
    """
    ret_bool = False
    permitted_words = {'source', 'region'}
    if grouping is None:
        ret_str = 'none'
    elif type(grouping) is str:
        grouping = grouping.lower()
        if grouping in permitted_words:
            ret_str = grouping
        else:
            return (False, None)
    elif type(grouping) is tuple:
        if  len(grouping) != 2:
            return (False, None)
        (first, last) = [l.lower() for l in grouping]
        if (first in permitted_words and
            last  in permitted_words and
            first != last):
                ret_str = '{},{}'.format(first, last)
        else:
            return(False, None)
    else:
        return(False, None)
    # good exit
    return (True, ret_str)

def get_ue_params_by(db_cursor, grouping=None) -> pd.DataFrame:
    """Возвращает параметры юнит-экономики в соответствии с заданной
    группировкой. Параметры: 
    1) Объект для связи с БД
    2) Метод группировки. Одно из пяти значений:
      - None (default) -- Вызвать ф-ю get_globals() и вернуть её результат.
      - 'source' -- юнит-экономика (UE) в разрезе методов привлечения.
      - 'region' -- UE в разрезе регионов проживания пользователей.
      - ('source', 'region') -- группировка по методу, потом по региону.
      - ('region', 'source') -- группировка по региону и методу.

    """
    if grouping is None:
        return get_globals(db_cursor)

    (Result, group_by) = check_ue_grouping(grouping)
    if Result is False:
        print('*ERR* Incorrect grouping specified')
        return None
    req = METRICS_BY_PARAM_TMPL.format(group_by)
    # print ("*DBG* database request:\n", req)
    db_cursor.execute(req)
    ue_grouped = db_cursor.fetchall()
    return pd.DataFrame(ue_grouped)

def get_data_by_source(db_cursor: psycopg2.extensions.cursor) -> pd.DataFrame:
    """Запрос общих данных по всем регионам, для разных методов привлечения
    пользователей.
    Параметры: 1) Курсор для доступа к БД
    Возвращает: дата-фрейм, по горизонтали переменные, 
        по вертикали методы привлечения
    Использует глобальную константу METRIC_NAMES
    """
    hdrs = ['source']
    hdrs.extend(METRIC_NAMES)
    source_df = get_ue_params_by(db_cursor, 'source')
    source_df.columns = hdrs
    source_df.set_index('source', inplace=True)
    return(source_df)

def get_data_by_region(db_cursor: psycopg2.extensions.cursor) -> pd.DataFrame:
    """Запрос общих данных по различным регионам, без разделения
    методов привлечения пользователей.
    Параметры: 1) Курсор для доступа к БД
    Возвращает: дата-фрейм, по горизонтали метрики UE, по вертикали регионы
    Использует глобальную константу METRIC_NAMES
    """
    hdrs = ['region']
    hdrs.extend(METRIC_NAMES)
    region_df = get_ue_params_by(db_cursor, 'region')
    region_df.columns = hdrs
    region_df.set_index(('region'), inplace=True)
    return region_df

def get_data_by_src_reg(db_cursor: psycopg2.extensions.cursor) -> pd.DataFrame:
    """Запрос большой таблицы по регионам и методам привлечения (3-мерный куб)
    Параметры: 1) Курсор для доступа к БД
    Возвращает: дата-фрейм, по горизонтали метрики UE, 
                            по вертикали двойной индекс: регионы и методы
    Использует глобальную константу METRIC_NAMES
    """
    headers= ['source', 'region']
    headers.extend(METRIC_NAMES)
    src_reg_df = get_ue_params_by(db_cursor, ('source', 'region'))
    src_reg_df.columns = headers
    src_reg_df.set_index('source','region', inplace=True)
    return src_reg_df

def compute_ue_data(db_cursor: psycopg2.extensions.cursor):
    """расчёт данных юнит-экономики.  Параметры: объект 'cursor' для доступа к БД.
    Возвращает DICT, где ключи - имена таблиц для печати, а значения - сами эти
    таблицы (data frame-ы)
    """
    ret_dict = {}
    ret_dict['Global data']    =  get_globals(db_cursor)
    ret_dict['Data by source'] = get_data_by_source(db_cursor)
    ret_dict['Data by region'] = get_data_by_region(db_cursor)
    ret_dict['Src and Region'] = get_data_by_src_reg(db_cursor)
    return ret_dict

def print_ue_data(data_frames_dict):
    """Just prints all DF's"""
    for k,v in data_frames_dict.items():
        print("\n\n", k, "\n", v)

def do_work():
    db_conn = None
    try:
        db_conn = psycopg2.connect(DB_CONNECT_STRING)
        cursor = db_conn.cursor()
        data_frames = compute_ue_data(cursor)
        print_ue_data(data_frames)
    except (psycopg2.Error) as error :
        print ("Error while working with PostgreSQL", error)
    except Exception as error:
        print ("Non-DB error, check your program: ", error)
    finally:
        if db_conn:
            cursor.close()
            db_conn.close()
            print("The database connection is closed")

# import argparse


if __name__ == "__main__":
    do_work()
    exit(0)


# access to DF: srdf.loc[:,['region','apc']]
#
#
