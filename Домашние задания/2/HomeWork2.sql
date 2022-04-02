-- Задание 2

-- Тестовые выборки
/*
select count(*) from HW2_CLIENT;
select count(*) from HW2_TRANSACTIONS;
select * from HW2_CLIENT;
select * from HW2_TRANSACTIONS;
select * from HW2_LOCATORS;
select * from HW2_CURRENCY_TYPEs;
select * from HW2_CURRENCY_EXCHANGE;
*/

--1+
/*Для каждого клиента посчитать кол-во и сумму транзакций в рублях
  (для перевода из других валют в рубли следует использовать таблицу HW2_CURRENCY_EXCHANGE посредством join);*/

--!!! можно было создать представление (view) вместо with, но суть от этого не поменяется...
--Вариант 1
select c.id, c.LASTNAME, c.NAME, c.CITY, count(t.CLIENT_ID) t_cnt, sum(nvl(t.MONEY_AMOUNT*ce.coef,0)) t_sum
from
    HW2_CLIENT c left join HW2_TRANSACTIONS t on c.ID = t.CLIENT_ID
    left join HW2_CURRENCY_EXCHANGE ce on t.CURRENCY_ID = ce.id and
                                          exists (select 1 from HW2_CURRENCY_TYPES ct where ce.TO_CURRENCY_ID = ct.id and ct.TITLE='Рубль')
group by c.id, c.LASTNAME, c.NAME, c.CITY
order by c.id;

--Вариант 2 (далее использовал его...)
with T_CURRENCY_EXCHANGE as (
    select ce.ID t_fr, ct.ID t_to, ct.TITLE t_tonm, ce.COEF coef
    from
        HW2_CURRENCY_EXCHANGE ce inner join HW2_CURRENCY_TYPES ct on ce.TO_CURRENCY_ID = ct.id
)
select c.id, c.LASTNAME, c.NAME, c.CITY, count(t.CLIENT_ID) t_cnt, sum(nvl(t.MONEY_AMOUNT*tce.coef,0)) t_sum
from
    HW2_CLIENT c left join HW2_TRANSACTIONS t on c.ID = t.CLIENT_ID
    left join T_CURRENCY_EXCHANGE tce on t.CURRENCY_ID = tce.t_fr and tce.t_tonm='Рубль'
group by c.id, c.LASTNAME, c.NAME, c.CITY
order by c.id;

--2+
/*Для каждого клиента вывести его email и телефон.
  Если контакт не найден, то в поле необходимо проставить ‘нет данных’;*/
select c.id,c.LASTNAME,c.NAME, c.CITY, trim(nvl(l.EMAIL,'нет данных')) eml, nvl(l.PHONE,'нет данных') tel
from
    HW2_CLIENT c left join HW2_LOCATORS l on c.LOCATOR_ID = l.LOCATOR_ID
order by c.id;

--Подготовка к итоговой выборке: обогощенная контактами:
with T_CURRENCY_EXCHANGE as (
    select ce.ID t_fr, ct.ID t_to, ct.TITLE t_tonm, ce.COEF coef
    from
        HW2_CURRENCY_EXCHANGE ce inner join HW2_CURRENCY_TYPES ct on ce.TO_CURRENCY_ID = ct.id
)
select c.id, c.LASTNAME, c.NAME, c.CITY, trim(nvl(l.EMAIL,'нет данных')) eml, nvl(l.PHONE,'нет данных') tel, count(t.CLIENT_ID) t_cnt, sum(nvl(t.MONEY_AMOUNT*tce.coef,0)) t_sum
from
    HW2_CLIENT c left join HW2_TRANSACTIONS t on c.ID = t.CLIENT_ID
    left join T_CURRENCY_EXCHANGE tce on t.CURRENCY_ID = tce.t_fr and tce.t_tonm='Рубль'
    left join HW2_LOCATORS l on c.LOCATOR_ID = l.LOCATOR_ID
group by c.id, c.LOCATOR_ID, c.LASTNAME, c.NAME, c.CITY, l.EMAIL, l.PHONE
order by c.id;

-- 3+
/*Для каждого клиента необходимо рассчитать флаг наличия хотя бы одного контакта:
  если хотя бы один контакт найден, то флаг принимает значение ‘Y’, иначе ‘N’*/
--exists отработает до 1 вхождения...т.о. дешевле чем count
select c.id,c.LASTNAME,c.NAME,
       nvl((select 'Y' from dual where exists (select 1 from HW2_LOCATORS l2 where l2.LOCATOR_ID=c.LOCATOR_ID and coalesce(l2.PHONE,l2.EMAIL) is not null)),'N') flgc
from
    HW2_CLIENT c
order by c.id;

--Подготовка к итоговой выборке: обогощенная флагом кол-ва контактов:
select T.ID, T.LASTNAME, T.NAME,
       nvl((select 'Y' from dual where exists (select 1 from HW2_LOCATORS l2 where l2.LOCATOR_ID=T.LOCATOR_ID and coalesce(l2.PHONE,l2.EMAIL) is not null)),'N') flgc,
       T.eml, T.tel, T.t_cnt, T.t_sum
from (with T_CURRENCY_EXCHANGE as (
        select ce.ID t_fr, ct.ID t_to, ct.TITLE t_tonm, ce.COEF coef
        from
            HW2_CURRENCY_EXCHANGE ce inner join HW2_CURRENCY_TYPES ct on ce.TO_CURRENCY_ID = ct.id
)
select c.id, c.LOCATOR_ID, c.LASTNAME, c.NAME, trim(nvl(l.EMAIL,'нет данных')) eml, nvl(l.PHONE,'нет данных') tel, count(t.CLIENT_ID) t_cnt, sum(nvl(t.MONEY_AMOUNT*tce.coef,0)) t_sum
from
    HW2_CLIENT c left join HW2_TRANSACTIONS t on c.ID = t.CLIENT_ID
    left join T_CURRENCY_EXCHANGE tce on t.CURRENCY_ID = tce.t_fr and tce.t_tonm='Рубль'
    left join HW2_LOCATORS l on c.LOCATOR_ID = l.LOCATOR_ID
group by c.id, c.LOCATOR_ID, c.LASTNAME, c.NAME, l.EMAIL, l.PHONE
order by c.id) T;

--4+
/*Для каждого клиента рассчитать флаг ‘Кол-во транзакций более 5’:
  если было совершено более 5 транзакций, то флаг принимает значение ‘Y’, иначе ‘N’;*/
select c.id,c.LASTNAME,c.NAME,
       case when count(t.CLIENT_ID) > 5 then 'Y' else 'N' end flgt
from
    HW2_CLIENT c left join  HW2_TRANSACTIONS t on c.ID = t.CLIENT_ID
group by c.id,c.LASTNAME,c.NAME
order by c.id;

--Подготовка к итоговой выборке: обогощенная флагом кол-ва транзакций:
select T.ID, T.LASTNAME, T.NAME,
       nvl((select 'Y' from dual where exists (select 1 from HW2_LOCATORS l2 where l2.LOCATOR_ID=T.LOCATOR_ID and coalesce(l2.PHONE,l2.EMAIL) is not null)),'N') flgc,
       T.eml, T.tel,
       case when T.t_cnt > 5 then 'Y' else 'N' end flgt,
       T.t_cnt, T.t_sum
from (with T_CURRENCY_EXCHANGE as (
        select ce.ID t_fr, ct.ID t_to, ct.TITLE t_tonm, ce.COEF coef
        from
            HW2_CURRENCY_EXCHANGE ce inner join HW2_CURRENCY_TYPES ct on ce.TO_CURRENCY_ID = ct.id
)
select c.id, c.LOCATOR_ID, c.LASTNAME, c.NAME, trim(nvl(l.EMAIL,'нет данных')) eml, nvl(l.PHONE,'нет данных') tel, count(t.CLIENT_ID) t_cnt, sum(nvl(t.MONEY_AMOUNT*tce.coef,0)) t_sum
from
    HW2_CLIENT c left join HW2_TRANSACTIONS t on c.ID = t.CLIENT_ID
    left join T_CURRENCY_EXCHANGE tce on t.CURRENCY_ID = tce.t_fr and tce.t_tonm='Рубль'
    left join HW2_LOCATORS l on c.LOCATOR_ID = l.LOCATOR_ID
group by c.id, c.LOCATOR_ID, c.LASTNAME, c.NAME, l.EMAIL, l.PHONE
order by c.id) T;

--5+
/*Для выгрузки подготовить список клиентов, для которых указан хотя бы 1 контакт
  и совершивших более 5 транзакций. В выгрузке должны содержаться следующие поля:*/

--!!! Представления (view) как обычные, так и материализованные не использовал. По сути это была бы подсказка решения всем из группы...
--!!! а так можно было create or replace view SNOW_TASK5 as ... или materialized view ...

select T.ID, T.LASTNAME, T.NAME, T.tel, T.eml, T.t_sum
from (with T_CURRENCY_EXCHANGE as (
        select ce.ID t_fr, ct.ID t_to, ct.TITLE t_tonm, ce.COEF coef
        from
            HW2_CURRENCY_EXCHANGE ce inner join HW2_CURRENCY_TYPES ct on ce.TO_CURRENCY_ID = ct.id
)
select c.id, c.LOCATOR_ID, c.LASTNAME, c.NAME, trim(nvl(l.EMAIL,'нет данных')) eml, nvl(l.PHONE,'нет данных') tel, count(t.CLIENT_ID) t_cnt, sum(nvl(t.MONEY_AMOUNT*tce.coef,0)) t_sum
from
    HW2_CLIENT c left join HW2_TRANSACTIONS t on c.ID = t.CLIENT_ID
    left join T_CURRENCY_EXCHANGE tce on t.CURRENCY_ID = tce.t_fr and tce.t_tonm='Рубль'
    left join HW2_LOCATORS l on c.LOCATOR_ID = l.LOCATOR_ID
group by c.id, c.LOCATOR_ID, c.LASTNAME, c.NAME, l.EMAIL, l.PHONE
order by c.id) T
where
    exists (select 1 from HW2_LOCATORS l2 where l2.LOCATOR_ID=T.LOCATOR_ID and coalesce(l2.PHONE,l2.EMAIL) is not null) and
    T.t_cnt > 5
order by T.id;
