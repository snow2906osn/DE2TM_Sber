/*
 Удалим таблицы, если есть
 */
 /*
drop table de2tm.snow_rep_fraud;
drop table de2tm.snow_stg_pssprt_blcklst;
drop table de2tm.snow_dwh_fact_pssprt_blcklst;
drop table de2tm.snow_meta_pssprt_blcklst;
drop table de2tm.snow_stg_transactions;
drop table de2tm.snow_dwh_fact_transactions;
drop table de2tm.snow_meta_transactions;
drop table de2tm.snow_stg_terminals;
drop table de2tm.snow_dwh_dim_terminals_hist;
drop table de2tm.snow_stg_delete_terminals;
drop table de2tm.snow_meta_terminals;
drop table de2tm.snow_stg_cards;
drop table de2tm.snow_dwh_dim_cards_hist;
drop table de2tm.snow_stg_delete_cards;
drop table de2tm.snow_meta_cards;
drop table de2tm.snow_stg_accounts;
drop table de2tm.snow_dwh_dim_accounts_hist;
drop table de2tm.snow_stg_delete_accounts;
drop table de2tm.snow_meta_accounts;
drop table de2tm.snow_stg_clients;
drop table de2tm.snow_dwh_dim_clients_hist;
drop table de2tm.snow_stg_delete_clients;
drop table de2tm.snow_meta_clients;
*/

/*
 Создание таблицы с отчетом
 */
create table de2tm.snow_rep_fraud
(event_dt timestamp,                            --как в таблице transactions.trans_date
 passport varchar2(15),                         --размерность по bank.clients
 fio varchar2(302),                             --размерность по сумме 3 полей+2 пробела из bank.clients LAST_NAME||' '||FIRST_NAME||' '||PATRONYMIC = 100+100+100+2
 phone varchar2(20),                            --размерность по bank.clients
 event_type varchar2(255),
 report_dt date);

/*
Создаем комплект "стэджинг", "приемник", "метаданные"
поля и наименования по ER-диаграмме
*/
--"стэджинг"
create table de2tm.snow_stg_pssprt_blcklst      --название идентичное с dwh_fact, проще работать
(passport_num varchar2(15),                     --размерность по bank.clients
 entry_dt date);
--"приемник"
create table de2tm.snow_dwh_fact_pssprt_blcklst --из-за ограничений, сократим наименование
(passport_num varchar2(15),                     --размерность по bank.clients
 entry_dt date);
--"метаданные"
create table de2tm.snow_meta_pssprt_blcklst     --название идентичное с dwh_fact, проще работать
(last_update_dt date);                          --для 12c можно было бы default on null to_date('01-01-1900','DD-MM-YYYY'), но у нас v$version показывает 11.2.0.1.0

--"стэджинг"
create table de2tm.snow_stg_transactions
(trans_id varchar2(15),
 trans_date timestamp,
 card_num varchar2(20),                         --размерность по bank.cards
 oper_type varchar2(8),
 amt decimal(9,2),
 oper_result varchar2(7),
 terminal varchar2(5));                         --т.к. в таблице terminals есть аналогичное поле, вычислим по максимальной длинне
--"приемник"
create table de2tm.snow_dwh_fact_transactions
(trans_id varchar2(15),
 trans_date timestamp,
 card_num varchar2(20),                         --размерность по bank.cards
 oper_type varchar2(8),
 amt decimal(9,2),
 oper_result varchar2(7),
 terminal varchar2(5));                         --т.к. в таблице terminals есть аналогичное поле, вычислим по максимальной длинне
--"метаданные"
create table de2tm.snow_meta_transactions
(last_update_dt timestamp);                     --для 12c можно было бы default on null to_date('01-01-1900','DD-MM-YYYY'), но у нас v$version показывает 11.2.0.1.0

--"стэджинг"
create table de2tm.snow_stg_terminals
(terminal_id varchar2(5),                       --по размерности значения в xlsx+округление
 terminal_type varchar2(3),                     --по размерности значения в xlsx+округление
 terminal_city varchar2(100),                    --по размерности значения в xlsx+округление т.к. UTF-8 длинна поля*2 (20*2)
 terminal_address varchar2(100));               --по размерности значения в xlsx+округление т.к. UTF-8 длинна поля*2 (50*2)
--"приемник"
create table de2tm.snow_dwh_dim_terminals_hist
(terminal_id varchar2(5),                       --по размерности значения в xlsx+округление
 terminal_type varchar2(3),                     --по размерности значения в xlsx+округление
 terminal_city varchar2(100),                    --по размерности значения в xlsx+округление т.к. UTF-8 длинна поля*2 (20*2)
 terminal_address varchar2(100),                --по размерности значения в xlsx+округление т.к. UTF-8 длинна поля*2 (50*2)
 effective_from date,
 effective_to date default(to_date('31-12-5999','DD-MM-YYYY')),
 deleted_flg int default(0));
--"удаление"
create table de2tm.snow_stg_delete_terminals
(terminal_id varchar2(5));                      --по размерности значения в xlsx+округление
--"метаданные"
create table de2tm.snow_meta_terminals
(last_update_dt date);                          --для 12c можно было бы default on null to_date('01-01-1900','DD-MM-YYYY'), но у нас v$version показывает 11.2.0.1.0

--"стэджинг"
create table de2tm.snow_stg_cards
(card_num varchar2(20),
 account varchar2(20),
 create_dt date,                                --в источнике bank.cards присутствует, не меняем
 update_dt date);                               --в источнике bank.cards присутствует, не меняем
--"приемник"
create table de2tm.snow_dwh_dim_cards_hist
(card_num varchar2(20),
 account_num varchar2(20),
 effective_from date,
 effective_to date default(to_date('31-12-5999','DD-MM-YYYY')),
 deleted_flg int default(0));
--"удаление"
create table de2tm.snow_stg_delete_cards
(card_num varchar2(20));
--"метаданные"
create table de2tm.snow_meta_cards
(last_update_dt date);                          --для 12c можно было бы default on null to_date('01-01-1900','DD-MM-YYYY'), но у нас v$version показывает 11.2.0.1.0

--"стэджинг"
create table de2tm.snow_stg_accounts
(account_num varchar2(20),                      --в источнике bank.accounts называется иначе, но "Имена полей менять нельзя". Делаем как в ER
 valid_to date,
 client	varchar2(20),
 create_dt date,                                --в источнике bank.cards присутствует, не меняем
 update_dt date);                               --в источнике bank.cards присутствует, не меняем
--"приемник"
create table de2tm.snow_dwh_dim_accounts_hist
(account_num varchar2(20),                      --в источнике bank.accounts называется иначе, но "Имена полей менять нельзя". Делаем как в ER
 valid_to date,
 client	varchar2(20),
 effective_from date,
 effective_to date default(to_date('31-12-5999','DD-MM-YYYY')),
 deleted_flg int default(0));
--"удаление"
create table de2tm.snow_stg_delete_accounts
(account_num varchar2(20));
--"метаданные"
create table de2tm.snow_meta_accounts
(last_update_dt date);                          --для 12c можно было бы default on null to_date('01-01-1900','DD-MM-YYYY'), но у нас v$version показывает 11.2.0.1.0

--"стэджинг"
create table de2tm.snow_stg_clients
(client_id	varchar2(20),
 last_name varchar2(100),
 first_name varchar2(100),
 patronymic varchar2(100),
 date_of_birth date,
 passport_num varchar2(15),
 passport_valid_to date,
 phone varchar2(20),
 create_dt date,                                --в источнике bank.cards присутствует, не меняем
 update_dt date);                               --в источнике bank.cards присутствует, не меняем
--"приемник"
create table de2tm.snow_dwh_dim_clients_hist
(client_id	varchar2(20),
 last_name varchar2(100),
 first_name varchar2(100),
 patronymic varchar2(100),
 date_of_birth date,
 passport_num varchar2(15),
 passport_valid_to date,
 phone varchar2(20),
 effective_from date,
 effective_to date default(to_date('31-12-5999','DD-MM-YYYY')),
 deleted_flg int default(0));
--"удаление"
create table de2tm.snow_stg_delete_clients
(client_id varchar2(20));
--"метаданные"
create table de2tm.snow_meta_clients
(last_update_dt date);                          --для 12c можно было бы default on null to_date('01-01-1900','DD-MM-YYYY'), но у нас v$version показывает 11.2.0.1.0

--Заполнение приемников. Т.к. эти таблицы уже в наличии, загрузим их сразу, но в .py их все равно отрабатываем при каждом запуске.
insert into de2tm.snow_dwh_dim_cards_hist(card_num, account_num, effective_from)
    select trim(bc.card_num), bc.account, coalesce(bc.update_dt,bc.create_dt) from bank.cards bc;

insert into de2tm.snow_dwh_dim_accounts_hist(account_num, valid_to, client, effective_from)
    select ba.account, ba.valid_to, ba.client, coalesce(ba.update_dt,ba.create_dt) from bank.accounts ba;

insert into de2tm.snow_dwh_dim_clients_hist(client_id, last_name, first_name, patronymic,
                                            date_of_birth, passport_num, passport_valid_to,
	                                        phone, effective_from)
    select bcl.client_id, bcl.last_name, bcl.first_name, bcl.patronymic, bcl.date_of_birth, bcl.passport_num, bcl.passport_valid_to,
           bcl.phone, coalesce(bcl.update_dt,bcl.create_dt) from bank.clients bcl;

--Первоначальное заполнение мета таблиц
insert into de2tm.snow_meta_cards(last_update_dt)
    select max(dch.effective_from) from de2tm.snow_dwh_dim_cards_hist dch;

insert into de2tm.snow_meta_accounts(last_update_dt)
	select max(dah.effective_from) from de2tm.snow_dwh_dim_accounts_hist dah;

insert into de2tm.snow_meta_clients(last_update_dt)
	select max(dclih.effective_from) from de2tm.snow_dwh_dim_clients_hist dclih;

insert into de2tm.snow_meta_pssprt_blcklst(last_update_dt) values(to_date('01-01-1900','DD-MM-YYYY'));
insert into de2tm.snow_meta_transactions(last_update_dt) values(to_date('01-01-1900','DD-MM-YYYY'));
insert into de2tm.snow_meta_terminals(last_update_dt) values(to_date('01-01-1900','DD-MM-YYYY'));
commit;