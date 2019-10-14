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
            c1                    as c1,
            avp  * apc            as arpc,
            avp  * apc * c1       as arpu,
            (avp * apc * c1)/cpa  as romi
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
    source_df.set_index(source_df.index.map(lambda x: x.strip()), inplace=True)
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
    region_df.set_index(region_df.index.map(lambda x: x.strip()), inplace=True)
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
    src_reg_df.set_index(['source','region'], inplace=True)
    # trim spaces in indexes (left from 'STR' type of DB)
    src_reg_df.set_index(src_reg_df.index.map(lambda x: (x[0].strip(),
            x[1].strip())), inplace=True)
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

def compute_ue_by_param(srdf: "source and region DF",
                        src_df: "totals by source DF",
                        reg_df: "totals by region DF") -> list:

    """Нарезка большого датафрейма на слайсы по параметрам Unit Economy.
    К результирующим датафреймам подклеиваются соответствующие столбец из src_df
    и строка из reg_df в качестве "сводных" значений
    """
    results = []
    for param in srdf.columns:
        # Перебор колонок
        param_df = make_wide_df_with_totals(srdf.loc[:,param],
                                            src_df.loc[:,param],
                                            reg_df.loc[:,param])
        results.append(param_df)
    return results

def make_wide_df_with_totals(tall_df:pd.DataFrame,
                             src_series: pd.Series,
                             reg_series:pd.Series) -> pd.DataFrame:
    """Разворачивает датафрейм в широкий формат,  добавляет справа колонку
    "total_source" из Series 'src_column', снизу строку 'total_region' из
    Series region_column
    Параметры:
    Возвращает: результирующий датафрейм
    """
    tall_df = tall_df.unstack()
    tall_df = tall_df.assign(total_source=src_series)
    tall_df = tall_df.append(reg_series.rename('total_region'))
    return tall_df

def print_ue_data(data_frames_dict):
    """Just prints all DF's"""
    for k,v in data_frames_dict.items():
        print("\n\n", k, "\n", v.to_csv())

def do_work():
    db_conn = None
    try:
        db_conn = psycopg2.connect(DB_CONNECT_STRING)
        cursor = db_conn.cursor()
        data_frames = compute_ue_data(cursor)
        print_ue_data(data_frames)
        by_param = compute_ue_by_param(data_frames['Src and Region'],
                                       data_frames['Data by source'],
                                       data_frames['Data by region'])
        print("\n\n".join([df.to_csv() for df in by_param]))
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


# access to DF: srdf.loc[:,'apc']]
# выделение метрики и её конверсия в широкий формат:
# c1=srdf.loc[:,'c1%'].unstack()
#
# Добавление новой колонки:

# In [54]: c1.assign(tot_src=src.loc[:,'c1%'])
# Out[54]:
# region                           ekb                   moscow                     orel                      spb                 vladimir                volgograd                  tot_src
# source
# cpc_adwords  69.73684210526315789500  67.60563380281690140800  68.78980891719745222900  65.21739130434782608700  67.96875000000000000000  68.12500000000000000000  67.88888888888888888900
# cpc_direct   72.85714285714285714300  65.87301587301587301600  63.15789473684210526300  72.53521126760563380300  60.00000000000000000000  75.15923566878980891700  68.47697756788665879600
# seo          67.62589928057553956800  70.90909090909090909100  68.07228915662650602400  63.79310344827586206900  76.22377622377622377600  73.33333333333333333300  69.85294117647058823500
# smm          60.30534351145038167900  66.87116564417177914100  68.71165644171779141100  70.86092715231788079500  71.59090909090909090900  70.12987012987012987000  68.33688699360341151400
#
# Добавление новой строки из таблицы регионов:
#
# In [64]: c1.append(reg.loc[:,'c1%'].rename('tot_region'))
# Out[64]:
# region                           ekb                   moscow                     orel                      spb                 vladimir                volgograd
# source
# cpc_adwords  69.73684210526315789500  67.60563380281690140800  68.78980891719745222900  65.21739130434782608700  67.96875000000000000000  68.12500000000000000000
# cpc_direct   72.85714285714285714300  65.87301587301587301600  63.15789473684210526300  72.53521126760563380300  60.00000000000000000000  75.15923566878980891700
# seo          67.62589928057553956800  70.90909090909090909100  68.07228915662650602400  63.79310344827586206900  76.22377622377622377600  73.33333333333333333300
# smm          60.30534351145038167900  66.87116564417177914100  68.71165644171779141100  70.86092715231788079500  71.59090909090909090900  70.12987012987012987000
# tot_region   67.79359430604982206400  67.95302013422818791900  67.24137931034482758600  67.83439490445859872600  69.32409012131715771200  71.69811320754716981100


