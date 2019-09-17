# Когортный анализ по неделям. Заметки и код

Начало отсчёта 1 января 2014, окончание 5 марта 2014. 10 недель (1-10)

## Когорта 1 недели.

Кол-во поcетителей: 381.

```
select count(distinct uid) from prj1.user where start_week=1;
```

Кол-во покупателей за время жизни когорты: 267.

```
select count(distinct l.uid)
from 
        prj1.log as l 
        left join 
        prj1.user as u 
        on l.uid=u.uid 
where 
        u.start_week=1 
        and 
        l.sum is not null;
```

Примерно 0.701 или 70.1%.

Количество сделок на одного пользователя из этой когорты легко получается из предыдущего запроса:

```
select count(l.uid) 
from prj1.log as l left join prj1.user as u on l.uid=u.uid 
where u.start_week=1 and l.sum is not null;
```

Получается 701, что даёт APC в 2.6255.

Считаем сумму всех сделок и средний чек.  Тут я решил, что мне `left join` не нужен, и достаточно
`natural join`.

```
select sum(sum) from prj1.log as l natural join prj1.user as u where u.start_week=1 and l.sum is not null;
```

Получается сумма сделок 628578 и средний чек round(628578.0/701,2) = 896.69.

С учётом этих данных валовая прибыль на клиента (Average gross profit per customer) будет 2354.23 и
ARPU при такой высокой конверсии достигнет 1649.82.

Расходная часть: суммарные расходы на привлечение пользователей 1 когорты и стоимость привлечения посетителя:

```
dgolub=> select sum(cost) from prj1.user where start_week=1;
  sum   
--------
 134820
(1 row)

dgolub=> select sum(cost)/count(uid) as ARPU from prj1.user where start_week=1;
       arpu       
------------------
 353.858267716535
```

## 2  и последующий недели

Мне надоело запускать команды руками снова и снова, и я пошёл двумя путями: группировки в SQl и запросы к БД из внешней программы
на Python.  Например, количество посетителей по всем неделям получается запросом:
```
dgolub=> select start_week,count(uid) from prj1.user where start_week is not null group by start_week order by start_week;
 start_week | count 
------------+-------
          1 |   381
          2 |   476
          3 |   340
          4 |   347
          5 |   495
          6 |   548
          7 |   302
          8 |   403
          9 |   329
         10 |    16
(10 rows)
```

## Количество покупателей по когортам

```
select u.start_week,count(distinct u.uid)
from 
	prj1.log as l natural join prj1.user as u 
where 
	u.start_week is not null and
	l.sum is not null and
	l.sum > 0 
group by u.start_week 
order by u.start_week;
```

То же, с разбивкой по неделям покупки:

```
select u.start_week, l.week,count(distinct u.uid)
from 
	prj1.log as l natural join prj1.user as u 
where 
	u.start_week is not null and
	l.sum is not null and
	l.sum > 0 
group by u.start_week, l.week 
order by u.start_week, l.week;
```

Средний чек:
```
select u.start_week, avg(l.sum) as avp 
from
	prj1.log as l natural join prj1.user as u 
where l.sum is not null
group by u.start_week
order by start_week;

start_week |       avp        
------------+------------------
          1 | 896.687589158345
          2 |  852.65548098434
          3 | 886.500851788756
          4 | 865.677265500795
          5 | 893.192401960784
          6 | 860.355408388521
          7 | 867.108910891089
          8 | 861.261450381679
          9 |  866.37216828479
         10 | 852.571428571429

```

Количество посетителей, расходы на них, стоимость 1 посетителя:

```
dgolub=> select start_week,count(distinct uid) as b, sum(cost) as ac, sum(cost)/count(distinct uid) as cpa from prj1.user group by start_week order by start_week;
 start_week |  b  |   ac   |       cpa        
------------+-----+--------+------------------
          1 | 381 | 134820 | 353.858267716535
          2 | 476 | 165191 | 347.039915966387
          3 | 340 | 120432 | 354.211764705882
          4 | 347 | 121212 | 349.314121037464
          5 | 495 | 175685 | 354.919191919192
          6 | 548 | 189669 | 346.111313868613
          7 | 302 | 106530 | 352.748344370861
          8 | 403 | 138506 | 343.687344913151
          9 | 329 | 114109 | 346.835866261398
         10 |  16 |   5842 |          365.125
            | 273 |  94505 | 346.172161172161
(11 rows)
```

Заполнив таблицу с помощью Python-овской программы, столкнулся с двойным учётом покупателей. Видимо, нужно все покупки пользвателя относить
к той неделе, когда он купил что-нибудь в первый раз.  Для облегчения себе жизни создал таблицу с первыми покупками пользователей.

```
create table prj1.first_buy as 
select uid,min(date) as fpdate, min(week) as fpweek from prj1.log 
where sum is not null 
group by uid 
order by min(date);
```


Промежуточный результат — объединённая таблица с логом, несколько строк:

```
dgolub=> select *
from prj1.log as l
inner join
prj1.user as u on l.uid = u.uid
inner join
prj1.first_buy as f on l.uid = f.uid
limit 10;

 uid  |    date    | event_type | sum  | month | week | uid  |    source    |   region   | cost | first_visit | start_mon | start_week | uid  |   fpdate   | fpweek 
------+------------+------------+------+-------+------+------+--------------+------------+------+-------------+-----------+------------+------+------------+--------
   63 | 2014-01-02 | visit      |      |     1 |    1 |   63 | smm          | moscow     |  474 | 2014-01-01  |         1 |          1 |   63 | 2014-01-01 |      1
   82 | 2014-01-08 | visit      |      |     1 |    2 |   82 | seo          | moscow     |  398 | 2014-01-01  |         1 |          1 |   82 | 2014-01-08 |      2
   98 | 2014-01-08 | visit      |      |     1 |    2 |   98 | smm          | vladimir   |  286 | 2014-01-02  |         1 |          1 |   98 | 2014-01-02 |      1
  176 | 2014-01-19 | visit      |      |     1 |    3 |  176 | seo          | vladimir   |  477 | 2014-01-02  |         1 |          1 |  176 | 2014-01-10 |      2
 3334 | 2014-03-03 | visit      |      |     3 |   10 | 3334 | seo          | volgograd  |  396 | 2014-02-22  |         2 |          8 | 3334 | 2014-02-23 |      8
  256 | 2014-01-02 | visit      |      |     1 |    1 |  256 | cpc_adwords  | moscow     |  385 | 2014-01-02  |         1 |          1 |  256 | 2014-01-04 |      1
  256 | 2014-01-04 | visit      |      |     1 |    1 |  256 | cpc_adwords  | moscow     |  385 | 2014-01-02  |         1 |          1 |  256 | 2014-01-04 |      1
  256 | 2014-01-04 | purchase   | 1587 |     1 |    1 |  256 | cpc_adwords  | moscow     |  385 | 2014-01-02  |         1 |          1 |  256 | 2014-01-04 |      1
  268 | 2014-01-03 | visit      |      |     1 |    1 |  268 | seo          | vladimir   |  417 | 2014-01-01  |         1 |          1 |  268 | 2014-01-06 |      2
  268 | 2014-01-01 | visit      |      |     1 |    1 |  268 | seo          | vladimir   |  417 | 2014-01-01  |         1 |          1 |  268 | 2014-01-06 |      2
```

Тогда список пользователей из январской когорты, у которых первая покупка тоже пришлась на январь, выдаются так:

```
dgolub=> select count(distinct uid)
from prj1.log as l
inner join
prj1.user as u on l.uid = u.uid
inner join
prj1.first_buy as f on l.uid = f.uid
limit 10;
```

```
select count(distinct u.uid) from prj1.log as l left join prj1.user as u on l.uid = u.uid left join prj1.first_buy as b on l.uid = b.uid where sum is not null and u.start_week=1 and b.fpweek=1 and week=2;
```

UPD: можно выставить использование схемы по умолчанию и далее пользоваться короткими названиями таблиц.
```
SET search_path TO prj1;

dgolub=> \d
          List of relations
 Schema |   Name    | Type  | Owner  
--------+-----------+-------+--------
 prj1   | first_buy | table | dgolub
 prj1   | log       | table | dgolub
 prj1   | user      | table | dgolub
```

Разбивка покупателей по когортам и неделям, в которую была совершена первая покупка:

```
select u.start_week, b.fpweek, count(distinct u.uid) from prj1.log as l left join prj1.user as u on l.uid = u.uid left join prj1.first_buy as b on l.uid = b.uid where sum is not null group by u.start_week,b.fpweek order by u.start_week,b.fpweek;

start_week | fpweek | count 
------------+--------+-------
          1 |      1 |   188
          1 |      2 |    58
          1 |      3 |    13
          1 |      4 |     1
...
          8 |     10 |     7
          9 |      9 |   161
          9 |     10 |    30
         10 |     10 |     7
```
