# Размышления над задачей

## Знакомство с таблицей user

Размеры: 3910×4, поля (uid, source, region, cost).

uid:
  Возрастающее целое число от 1 до кол-ва пользователей, первичный ключ.

source:
  Видимо, источник привлечения пользователя. Принимает одно из значений: 
  (cpc_adwords, cpc_direct, seo, smm). Стоимость разных источников мало
  отличается, боксплоты и вайолины очень похожи.  Есть отличия по регионам.

  ```
  > ggplot(user, aes(source, cost)) + geom_boxplot()
  > ggplot(user, aes(source, cost)) + geom_violin()
  ```

 Количество пользователей, привлечённых разными способами:
 
 | Способ привлечения | Цена |
 |:-------------------|-----:|
 | cpc_adwords        | 958  |
 | cpc_direct         | 922  |
 | seo                | 1016 |
 | smm                | 1014 |

region:
  Регион пользователя. Есть 6 регионов, количество пользователей в них близко:

  | регион            | #users |
  |:------------------|-------:|
  | ekb               |  609   |
  | moscow            |  645   |
  | orel              |  687   |
  | spb               |  664   |
  | vladimir          |  619   |
  | volgograd         |  686   |

cost:
  По всей видимости, стоимость привлечения пользователя. В среднем стоимость привлечения незначительно
  изменяется от региона к региону, но есть заметные отличия по способам привлечения.  Медианная стоимость
  привлечения пользователя в разрезе способов привлечения и регионов:

|region       |  cpc_adwords |  cpc_direct | seo   | smm
|:------------|:-------------|:------------|:------|:--------------
|       ekb   |   343.5      |   354       | 365   | 358
|    moscow   |   359        |   350       | 366   | 337.5
|      orel   |   359.5      |   346.5     | 310.5 | 345
|       spb   |   356.5      |   349       | 339   | 370
|  vladimir   |   358        |   372       | 332   | 358
| volgograd   |   340        |   341       | 347.5 | 353


## Знакомство с таблицей log

Размыры: 35775×4, поля (uid, date, event_type, sum), где uid ссылка на идентификатор пользователя,
date в формате YYYY-MM-DD, event_type может быть "(visit|purchase)" и sum либо сумма покупки, либо NA.

Первичного ключа в таблице нет.

Диапазон дат: 1 января — 5 марта 2014

```
R> summary(log$date)
        Min.      1st Qu.       Median         Mean      3rd Qu.         Max. 
"2014-01-01" "2014-01-21" "2014-02-05" "2014-02-03" "2014-02-19" "2014-03-05" 
```

Даты в логе не отсортированы (довольно странно для лога :) )

Проверяю, нужна ли мне вообще колонка 'event_type'

```
dgolub=> select * from prj1.log where event_type='visit' and sum is not null;
 uid | date | event_type | sum 
-----+------+------------+-----
(0 rows)
```

Понятно, таких нет.  А покупка без суммы?

```
dgolub=> select * from prj1.log where event_type='purchase' and sum is null;
 uid | date | event_type | sum 
-----+------+------------+-----
(0 rows)
```

ОК, при визите не покупаем, при покупке всегда платим. Можно event type не
держать в таблице.

Юнит-экономику можно считать по месяцам или по неделям, для этого есть смысл
завести отдельные колонки в базе данных с номером месяца и номером недели.

## Первый визит пользователя

Для когортных расчётов нужно знать, когда пользователь пришёл первый раз, это
можно сделать, найдя минимальную дату его визита.

```{sql}
select u.uid, min(l.date)
  from prj1.user as u
  left join prj1.log as l
  on u.uid=l.uid group by u.uid 
  order by u.uid ;
```

Забавно, что для некоторых пользвателей визитов и покупок нет вообще.  Мы их
привлекли, этим всё и ограничилось.

Для когортного анализа полезны номера месяцев и недель. Можно нарезать эти
промежутки с помощью функций date_trunc (то есть привести к первой дате
интервала) или преобразовать в номер месяца (недели).  Нужную информацию можно
внести прямо в таблицу `log`.

```
select date, date_part('month', date) as month, date_part('week', date) as week from prj1.log;
```

Добавлю колонки month & week. Следующим действием поставлю их после даты.

```
dgolub=> alter table prj1.log add column month int, add column week int;
ALTER TABLE

dgolub=> select * from prj1.log limit 5;

 uid |    date    | event_type | sum  | month | week 
-----+------------+------------+------+-------+------
 256 | 2014-01-02 | visit      |      |       |     
 256 | 2014-01-04 | visit      |      |       |     
 256 | 2014-01-04 | purchase   | 1587 |       |     
 268 | 2014-01-03 | visit      |      |       |     
 268 | 2014-01-01 | visit      |      |       |     
(5 rows)
```

Заполняю колонки с месяцем и неделей.

```
dgolub=> update prj1.log set week = date_part('week', date);
UPDATE 35775

dgolub=> select * from prj1.log limit 10 offset 124;

 uid |    date    | event_type | sum | month | week 
-----+------------+------------+-----+-------+------
  34 | 2014-01-03 | visit      |     |     1 |    1
  34 | 2014-01-05 | visit      |     |     1 |    1
  34 | 2014-01-06 | visit      |     |     1 |    2
  34 | 2014-01-03 | visit      |     |     1 |    1
  36 | 2014-01-07 | visit      |     |     1 |    2
  36 | 2014-01-02 | visit      |     |     1 |    1
  31 | 2014-01-05 | visit      |     |     1 |    1
  31 | 2014-01-05 | purchase   | 796 |     1 |    1
  31 | 2014-01-03 | visit      |     |     1 |    1
  31 | 2014-01-06 | visit      |     |     1 |    2
```

Создаю таблицу и начинаю заполнять юнит-экономику по месяцам и по неделям. У
нас 2 месяца и 10 недель (первая и последняя недели неполные, и оба месяца
аномальных: январь с новогодними каникулами и февраль короткий).

Заполнение юнит-экономики упёрлось в стоимость привлечения, для чего нужно понять,
когда этот пользователь впервые пришёл на сайт.  У нас есть данные визитов, но нужно
соединить их с таблицей пользователей.

Добавляю в таблицу пользователей поля для первого контакта, месяца первого контакта 
и номера недели первого контакта.

```
dgolub=> alter table prj1.user add column first_visit date, add column start_mon int, add column start_week int;
ALTER TABLE

dgolub=> select * from prj1.user limit 5;

 uid |    source    |   region   | cost | first_visit | start_mon | start_week 
-----+--------------+------------+------+-------------+-----------+------------
   1 | seo          | vladimir   |  321 |             |           |           
   2 | cpc_direct   | vladimir   |  228 |             |           |           
   3 | smm          | vladimir   |  436 |             |           |           
   4 | smm          | vladimir   |  464 |             |           |           
   5 | cpc_direct   | ekb        |  269 |             |           |           
(5 rows)

```

Как заполнить эти поля? Нужно слить две таблицы и сделать update в одной из них.
На [StackOverflow](https://stackoverflow.com/questions/1293330/how-can-i-do-an-update-statement-with-join-in-sql)
написано, что для PostgreSQL действует синтаксис:

```
update ud
  set ud.assid = s.assid
from sale s 
where ud.id = s.udid;
```

Для нашего случая это превращается в довольно запутанную форму, поскольку нужно ещё считать агрегатные
функции. Проще всего создать временную таблицу и удалить её после работы.

Сначала пытался создать таблицу в той же схеме, но это невозможно:

```
dgolub=> create temp table prj1.first_visit as 
	select u.uid, min(l.date) from prj1.user as u inner join prj1.log as l on u.uid=l.uid 
	group by u.uid order by u.uid ;
ERROR:  cannot create temporary relation in non-temporary schema
```

Правильная команда создаёт таблицу в схеме `public`.

```
create temp table first_visit as 
	select u.uid, min(l.date) 
	from 
		prj1.user as u 
		inner join 
		prj1.log as l 
		on u.uid=l.uid 
	group by u.uid
	order by u.uid ;
SELECT 3637
```

Я бездумно скопировал сюда пример, котоый был рассмотрен выше — с LEFT JOIN, а тут таблица `user`
и вовсе не нужна, все нужные данные уже в таблице `log`.  То же самое выдаёт команда попроще:

Таблица напрасно названа `first_visit`, у нас уже есть поле с таким именем.  Дропаю её и
создаю снова.

```
dgolub=> ... select uid, min(date) from prj1.log group by uid order by uid;
 uid |    min     
-----+------------
   1 | 2014-01-01
   2 | 2014-01-04
   3 | 2014-01-05
...
```

Таблица наследует типы полей от родительских, они не задаются.

```
dgolub=> \d first_visit

           Table "pg_temp_3.first_visit"
 Column |  Type   | Collation | Nullable | Default 
--------+---------+-----------+----------+---------
 uid    | integer |           |          | 
 min    | date    |           |          | 
```

Содержимое таблицы:

```
dgolub=> select * from first_visit limit 10;
 uid |    min     
-----+------------
   1 | 2014-01-01
   2 | 2014-01-04
   3 | 2014-01-05
   4 | 2014-01-02
   5 | 2014-01-01
   8 | 2014-01-02
   9 | 2014-01-01
  10 | 2014-01-03
  11 | 2014-01-01
  12 | 2014-01-02
(10 rows)
```

Пользователи 6 и 7, которые есть в таблице `user`, но которых нет в таблице `log`, сюда не попали,
так как их визитов не зарегистрировано.

```
dgolub=> update prj1.user
  set first_visit = fs.min
from first_seen as fs
where fs.uid = prj1.user.uid;
UPDATE 3637

dgolub=> select * from prj1.user limit 12;
 uid |    source    |   region   | cost | first_visit | start_mon | start_week 
-----+--------------+------------+------+-------------+-----------+------------
  51 | cpc_direct   | orel       |  438 | 2014-01-03  |           |           
  52 | smm          | spb        |  452 | 2014-01-01  |           |           
  53 | cpc_adwords  | spb        |  475 | 2014-01-01  |           |           
  54 | smm          | spb        |  245 | 2014-01-01  |           |           
  56 | cpc_adwords  | volgograd  |  365 | 2014-01-02  |           |           
   6 | smm          | moscow     |  414 |             |           |           
   7 | cpc_adwords  | moscow     |  258 |             |           |           
  57 | smm          | volgograd  |  430 | 2014-01-02  |           |           
  58 | cpc_direct   | volgograd  |  473 | 2014-01-02  |           |           
  59 | cpc_direct   | vladimir   |  373 | 2014-01-01  |           |           
  60 | smm          | vladimir   |  470 | 2014-01-01  |           |           
  61 | seo          | vladimir   |  409 | 2014-01-01  |           |           
(12 rows)
```

Номера месяцев и недель заполняю аналогично таблице лога.

```
dgolub=> update prj1.user set start_mon=date_part('month',first_visit);
UPDATE 3910

dgolub=> update prj1.user set start_week=date_part('week',first_visit);
UPDATE 3910
```

Немного произвольно я считаю, что тот месяц, когда пользователь первый раз посетил сайт,
и есть месяц его привлечения. Стоимость привлечания я отношу именно сюда.

Тогда стоимость привлечения по месяцам:

```
select sum(cost) from prj1.user where start_mon = 1;
```


14.10 обсудили немного результаты работы с маркетологом, основные результаты:
 - Смотреть на то, что даёт максимальный доход.  Это SEO и Google Ads,  из городов - Мск, Питер, Екб. Волгоград
 - SEO в Мск и Спб, Директ в СПб
 - Сделать разрез параметров UE по методам, 1 табличка на регион, сортировать по ROMI
 - Сделать аналогичный разрез по регионам, 1 табличка на метод, сортировать по ROMI
 - Топ источников по регионам, смотрим Contribution Margin
