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
