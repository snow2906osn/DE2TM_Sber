--1 Создать таблицу XXXX_CLIENT (в соответствии с ER-диаграммой), куда загрузить клиентов и информацию о них с вкладки client;
create table SNOW_CLIENT (
  id integer primary key,
  name varchar2(64),
  lastname varchar2(64),
  locator_id integer,
  city varchar2(64));

insert into SNOW_CLIENT (id, name, lastname, locator_id, city) values (1,'Александр','Иванов',1,'Уфа');
insert into SNOW_CLIENT (id, name, lastname, locator_id, city) values (2,'Борис','Калинин',7,'Ереван');
insert into SNOW_CLIENT (id, name, lastname, locator_id, city) values (3,'Петр','Суворов',2,'Казань');
insert into SNOW_CLIENT (id, name, lastname, locator_id, city) values (4,'Юрий','Сахаров',6,'Владимир');
insert into SNOW_CLIENT (id, name, lastname, locator_id, city) values (5,'Всеволод','Долгих',4,'Екатеринбург');
insert into SNOW_CLIENT (id, name, lastname, locator_id, city) values (6,'Александр','Виноградов',5,'Москва');
insert into SNOW_CLIENT (id, name, lastname, locator_id, city) values (7,'Николай','Николаев',2,'Нижний Новгород');
insert into SNOW_CLIENT (id, name, lastname, locator_id, city) values (8,'Ольга','Печуркина',1,'Санкт-Петербург');
insert into SNOW_CLIENT (id, name, lastname, locator_id, city) values (9,'Екатерина','Александрова',8,'Чебоксары');
insert into SNOW_CLIENT (id, name, lastname, locator_id, city) values (10,'Юлия','Абрикосова',8,'Москва');


--2 Переименовать столбец LASTNAME в LAST_NAME в таблице XXXX_CLIENT;
ALTER TABLE SNOW_CLIENT rename column lastname to last_name;

--3 Изменить тип данных поля CITY на varchar2 (100) в таблице XXXX_CLIENT;
ALTER TABLE SNOW_CLIENT modify (city varchar2(100));

--4 Создать представление XXXX_V_MOSCOW_CLIENT и записать туда всех клиентов из Москвы на основе созданной ранее таблицы CLIENT;
create or replace view SNOW_V_MOSCOW_CLIENT as
    select * from SNOW_CLIENT where city = 'Москва';

--5 Создать таблицу XXXX_CURRENCY_TYPES, куда загрузить данные с вкладки currency_types;
create table SNOW_CURRENCY_TYPE (
  id integer primary key,
  title varchar2(16));

insert into SNOW_CURRENCY_TYPE (id, title) values (1,'Рубль');
insert into SNOW_CURRENCY_TYPE (id, title) values (2,'Доллар');
insert into SNOW_CURRENCY_TYPE (id, title) values (3,'Евро');

--6 Создать представление XXXX_V_TRANSACTIONS, в которое вывести все рублевые и долларовые транзакции на основе данных таблицы TRANSACTIONS (уже прогружена в нашу схему);
/*select * from TRANSACTIONS;
create or replace view SNOW_V_TRANSACTIONS as
    select * from TRANSACTIONS where CURRENCY_id in (1,2);
*/

create or replace view SNOW_V_TRANSACTIONS as
    select t.* from TRANSACTIONS t, SNOW_CURRENCY_TYPE st where t.CURRENCY_id=st.id and st.title in ('Рубль','Доллар');

--7 Вывести все рублевые транзакции либо на очень маленькую сумму (меньше 10 рублей), либо на большую (больше 20 000 рублей);
select * from TRANSACTIONS t
where
  t.MONEY_AMOUNT < 10 or
  t.MONEY_AMOUNT > 20000;

--8 После всех изменений удаляем созданные объекты + объекты, созданные Вами на занятии.
drop view SNOW_V_TRANSACTIONS;
drop view SNOW_V_MOSCOW_CLIENT;
drop table SNOW_CURRENCY_TYPE;
drop table SNOW_CLIENT;
