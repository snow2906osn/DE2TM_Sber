#!/usr/bin/python

import pandas
import jaydebeapi
import os
import sys

if __name__ == '__main__':
    # Этап 1........................................................Работа с OS
    v_mysxm='SNOW'                                                  #моя группа талиц/файлов !!! в верхнем регистре !!!
    v_errors=0                                                      #если ошибок нет, то перенесем в архив файлы
    v_daemon=1                                                      #если 1, то пишем все выводы в лог-файл ...вместо stdout
    if v_daemon==1:
        flog=open('{}.log'.format(v_mysxm),'a')                     #открываем файл на добавление, на 'w' перезатрет лог, а у нас он в кроне...
        sys.stdout=flog                                             #Переопределим вывод в файл
    v_dir='/home/de2tm/SNOW'                                        #каталог источников. Укажем абсолютный, иначе могут быть проблемы с кроном
    #v_dir=''                                                        #каталог источников
    v_arxdir=''                                                     #Архив
    v_sqldir=''                                                     #SQL скрипт
    if len(v_dir)==0:
        v_dir=os.path.abspath(os.path.curdir)                       #Если не задали каталог, сделаем его в текущей
    os.chdir(v_dir)                                                 #Сменим путь..чтоб библиотека и файлы при выполнении от cron искались в нужном месте
    if len(v_arxdir)==0:
        v_arxdir=v_dir+'/archive'                                   #Если не задали каталог, сделаем его в текущей директории
    if len(v_sqldir)==0:
        v_sqldir=v_dir+'/sql_scripts'                               #Если не задали каталог, сделаем его в текущей директории
    if not os.path.isdir(v_arxdir):                                 #Если каталога с архивом нет, создадим
        try:
            os.mkdir(v_arxdir)                                      #считаем что по умолчанию есть права на запись в каталог
        except:
            print('Archive dir {} not created'.format(v_arxdir))
            v_arxdir=''
    v_list=[]                                                       #Определим переменную
    try:
        v_list=os.listdir(v_dir)                                    #создаем список из имен файлов
    except:
        print('Source dir {} not exists'.format(v_dir))
    v_list.sort()                                                   #сортируем список файлов
    v_data=''                                                       #определим переменную
    for v_fname in v_list:                                          #обходим все файлы в каталоге
        if v_fname[0:11]=='transaction' and v_fname[-4:]=='.csv' :  #список транзакций - факт в формате .csv
            v_data=v_fname[-12:-4]                                  #дата
            break                                                   #выйдем из просмотра списка, при нахождении 1-ой необходимой записи
    v_fl_pssp='passport_blacklist_{}.xlsx'.format(v_data)
    v_fl_term='terminals_{}.xlsx'.format(v_data)
    v_fl_tran='transactions_{}.csv'.format(v_data)

    try:                                                            #Проверим наличие файлов
        if v_fl_pssp not in v_list:
            raise Exception()
    except:
        print('Passport blacklist file {} not found'.format(v_fl_pssp))
        v_fl_pssp=''
    try:
        if v_fl_term not in v_list:
            raise Exception()
    except:
        print('Terminals file {} not found'.format(v_fl_term))
        v_fl_term=''
    try:
        if v_fl_tran not in v_list:
            raise Exception()
    except:
        print('Transactions file {} not found'.format(v_fl_tran))
        v_fl_tran=''
    v_stop=1
    if len(v_fl_pssp)>0 and len(v_fl_term)>0 and len(v_fl_tran)>0 and len(v_arxdir)>0:#Все файлы должны быть в каталоге для запуска обработчика
        v_stop=0
    if v_stop==1:                                                   #Если файлов нет выводим сообщение, а если есть то начинаем...
        print('Stop')
    else:
        print('Start')
        df_pssp=pandas.read_excel(v_fl_pssp, header=0)              #Загрузим файл паспортов
        df_term=pandas.read_excel(v_fl_term, header=0)              #Загрузим файл терминалов
        df_tran=pandas.read_csv(v_fl_tran, sep=';', decimal=',', header=0, skipinitialspace=True)#Загрузим файл транзакций. Вместо sep можно delimiter(alias4sep)

        #Этап 2.....................................................RDBMS Oracle
        conn=jaydebeapi.connect('oracle.jdbc.driver.OracleDriver','jdbc:oracle:thin:de2tm/xxxxxxxxxxxx@de-oracle.chronosavant.ru:1521/deoracle',
                                ['de2tm','xxxxxxxxxxxx'],'ojdbc8.jar')
        curs=conn.cursor()
        conn.jconn.setAutoCommit(False)                             #Отключим autocommit

        #Этап 2.1...................................................Проверяем наличие таблиц, чистим
        v_template="""select table_name from user_tables ut where instr(ut.table_name,'{}')>0 and instr(ut.table_name,'STG')>0""".format(v_mysxm)#DML для получения списка таблиц
        curs.execute(v_template)                                    #Получим список стеджинг таблиц
        v_lstg=[]                                                   #Определим переменную
        v_lstg=curs.fetchall()
        if len(v_lstg)>0:                                           #таблицы есть в схеме de2tm
            v_template='delete from de2tm.{} where 1=1'             #шаблон для удаления
            for v_tab in v_lstg:
                try:
                    curs.execute(v_template.format(v_tab[0]))       #подчистим данные в стеджинг таблицах
                except:
                    print('Stage data in {} delete error'.format(v_tab[0]))
        else:
            print('!!! Create tables, use file {}/create_ddl.sql'.format(v_sqldir))#Для создания таблиц
            v_errors=1
            sys.exit(200)                                           #завершим выполнение с кодом 200, по идее курсоры, соединения и дескриптор файла сами автоматически закроются

        #Этап 2.2...................................................Основной процесс
        #Подготовим данные
        v_curday='{}-{}-{}'.format(v_data[4:8], v_data[2:4], v_data[0:2])
        print('Data:{}'.format(v_curday))
        v_prevday=''
        v_nxtday=''
        try:
            v_template="""select to_date('{}','YYYY-MM-DD')-1 pday, to_date('{}','YYYY-MM-DD')+1 nday from dual""".format(v_curday,v_curday)#можно было через datetime.timedelta, но памяти по идее больше из-за импорта datetime
            curs.execute(v_template)                                #получим дату из meta таблицы
            v_dtget=curs.fetchone()
            v_prevday=str(v_dtget[0])[0:10]
            v_nxtday=str(v_dtget[1])[0:10]
        except:
            print('Get Prev & Next day error')
            v_prevday=v_curday
            v_nxtday=v_curday
        v_lupdt=''
        try:
            v_template="""select to_char(last_update_dt,'YYYY-MM-DD') from de2tm.{}_meta_pssprt_blcklst""".format(v_mysxm)
            curs.execute(v_template)                                #получим дату из meta таблицы
            v_lupdt=curs.fetchone()[0]
            if len(v_lupdt)==0:                                     #если проблема, то берем дату по названию файла
                raise Exception()
        except:
            v_lupdt=v_curday
        df_pssp=df_pssp.loc[df_pssp['date']>v_lupdt]                #берем только новые данные
        df_pssp['date']=df_pssp['date'].astype(str)                 #приведем к строке
        if not df_pssp.empty:                                       #если есть данные по паспортам
            try:
                curs.executemany("""insert into de2tm.{}_stg_pssprt_blcklst(entry_dt, passport_num)
                                    values(to_date(?,'YYYY-MM-DD'), ?)""".format(v_mysxm),df_pssp.values.tolist())
            except:
                print('Error insert Passport data in stage table')
                v_errors=1
        if not df_term.empty:                                       #если есть данные по паспортам
            try:
                curs.executemany("""insert into de2tm.{}_stg_terminals(terminal_id, terminal_type, terminal_city, terminal_address)
                                    values(?, ?, ?, ?)""".format(v_mysxm),df_term.values.tolist())
            except:
                print('Error insert Terminals data in stage table')
                v_errors=1
        if not df_tran.empty:                                       #если есть данные по паспортам
            try:
                curs.executemany("""insert into de2tm.{}_stg_transactions(trans_id, trans_date, amt, card_num, oper_type, oper_result, terminal)
                                    values (?, to_date(?, 'YYYY-MM-DD HH24:MI:SS'), ?, ?, ?, ?, ?)""".format(v_mysxm),df_tran.values.tolist())
            except:
                print('Error insert Transaction data in stage table')
                v_errors=1
        try:
            curs.execute("""insert into de2tm.{}_stg_accounts(account_num, valid_to, client, create_dt, update_dt)
                            select ba.account, ba.valid_to, ba.client, ba.create_dt, ba.update_dt
                            from bank.accounts ba
                                 inner join de2tm.{}_meta_accounts ma on coalesce(ba.update_dt,ba.create_dt)>ma.last_update_dt""".format(v_mysxm,v_mysxm))
        except:
            print('Error insert Accounts data in stage table')
            v_errors=1
        try:
            curs.execute("""insert into de2tm.{}_stg_cards(card_num, account, create_dt, update_dt)
                            select trim(bc.card_num), bc.account, bc.create_dt, bc.update_dt
                            from bank.cards bc
                                 inner join de2tm.{}_meta_cards mc on coalesce(bc.update_dt,bc.create_dt)>mc.last_update_dt""".format(v_mysxm,v_mysxm))
        except:
            print('Error insert Cards data in stage table')
            v_errors=1
        try:
            curs.execute("""insert into de2tm.{}_stg_clients(client_id, last_name, first_name, patronymic, date_of_birth,
                                                             passport_num, passport_valid_to, phone, create_dt, update_dt)
                            select bcl.client_id, bcl.last_name, bcl.first_name, bcl.patronymic, bcl.date_of_birth,
                                   bcl.passport_num, bcl.passport_valid_to, bcl.phone, bcl.create_dt, bcl.update_dt
                            from bank.clients bcl
                                 inner join {}_meta_clients mcl on coalesce(bcl.update_dt,bcl.create_dt)>mcl.last_update_dt""".format(v_mysxm,v_mysxm))
        except:
            print('Error insert Clients data in stage table')
            v_errors=1
        try:
            curs.execute("""insert into de2tm.{}_stg_delete_accounts(account_num)
                            select ba.account from bank.accounts ba""".format(v_mysxm))
        except:
            print('Error insert Accounts data in stage_delete table')
            v_errors=1
        try:
            curs.execute("""insert into de2tm.{}_stg_delete_cards(card_num)
                            select trim(bc.card_num) from bank.cards bc""".format(v_mysxm))
        except:
            print('Error insert Cards data in stage_delete table')
            v_errors=1
        try:
            curs.execute("""insert into de2tm.{}_stg_delete_clients(client_id)
                            select bcl.client_id from bank.clients bcl""".format(v_mysxm))
        except:
            print('Error insert Clients data in stage_delete table')
            v_errors=1
        try:
            curs.execute("""insert into de2tm.{}_dwh_fact_pssprt_blcklst(passport_num, entry_dt)
                            select pb.passport_num, pb.entry_dt from de2tm.{}_stg_pssprt_blcklst pb""".format(v_mysxm,v_mysxm))
        except:
            print('Error insert Passport Blacklist data in Fact table')
            v_errors=1
        try:
            curs.execute("""insert into de2tm.{}_dwh_fact_transactions(trans_id, trans_date, card_num, oper_type, amt, oper_result, terminal)
                            select st.trans_id, st.trans_date, st.card_num, st.oper_type, st.amt, st.oper_result, st.terminal
                            from de2tm.{}_stg_transactions st""".format(v_mysxm,v_mysxm))
        except:
            print('Error insert Transactions data in Fact table')
            v_errors=1
        try:
            curs.execute("""insert into de2tm.{}_dwh_dim_accounts_hist(account_num, valid_to, client, effective_from)
                            select sa.account_num, sa.valid_to, sa.client, coalesce(sa.update_dt,sa.create_dt)
                            from de2tm.{}_stg_accounts sa""".format(v_mysxm,v_mysxm))
        except:
            print('Error insert Accounts data in Dim table')
            v_errors=1
        try:
            curs.execute("""insert into de2tm.{}_dwh_dim_cards_hist(card_num, account_num, effective_from)
                            select trim(sc.card_num), sc.account, coalesce(sc.update_dt,sc.create_dt)
                            from de2tm.{}_stg_cards sc""".format(v_mysxm,v_mysxm))
        except:
            print('Error insert Cards data in Dim table')
            v_errors=1
        try:
            curs.execute("""insert into de2tm.{}_dwh_dim_clients_hist(client_id, last_name, first_name, patronymic, date_of_birth,
                                                                      passport_num, passport_valid_to, phone, effective_from)
                            select sc.client_id, sc.last_name, sc.first_name, sc.patronymic, sc.date_of_birth, sc.passport_num,
                                   sc.passport_valid_to, sc.phone, coalesce(sc.update_dt,sc.create_dt)
                            from de2tm.{}_stg_clients sc""".format(v_mysxm,v_mysxm))
        except:
            print('Error insert Clients data in Dim table')
            v_errors=1
        try:
            curs.execute("""insert into de2tm.{}_dwh_dim_terminals_hist(terminal_id, terminal_type, terminal_city, terminal_address,
                                                                        effective_from)	
                            select st.terminal_id, st.terminal_type, st.terminal_city, st.terminal_address, to_date('{}', 'YYYY-MM-DD')
                            from de2tm.{}_stg_terminals st
                                left join de2tm.{}_dwh_dim_terminals_hist dt on st.terminal_id = dt.terminal_id
                            where
                                dt.terminal_id is null or
                                ((st.terminal_city!=dt.terminal_city or st.terminal_address!=dt.terminal_address or st.terminal_type!=dt.terminal_type) and
                                dt.effective_to=to_date('5999-12-31','YYYY-MM-DD'))""".format(v_mysxm,v_nxtday,v_mysxm,v_mysxm))
        except:
            print('Error insert Terminals data in Dim table')
            v_errors=1
        try:
            curs.execute("""merge into de2tm.{}_dwh_dim_accounts_hist ah
                            using de2tm.{}_stg_accounts sa
                            on (ah.account_num=sa.account_num and ah.effective_from<coalesce(sa.update_dt, to_date('1900-01-01','YYYY-MM-DD')))
                            when matched then
                                update set ah.effective_to=sa.update_dt-1
                                where ah.effective_to=to_date('5999-12-31','YYYY-MM-DD')""".format(v_mysxm,v_mysxm))
        except:
            print('Error merge Accounts table')
            v_errors=1
        try:
            curs.execute("""merge into de2tm.{}_dwh_dim_cards_hist ch
                            using de2tm.{}_stg_cards sc
                            on (trim(ch.card_num)=trim(sc.card_num) and ch.effective_from<coalesce(sc.update_dt, to_date('1900-01-01','YYYY-MM-DD')))
                            when matched then
                                update set ch.effective_to=sc.update_dt-1
                                where ch.effective_to=to_date('5999-12-31','YYYY-MM-DD')""".format(v_mysxm,v_mysxm))
        except:
            print('Error merge Cards table')
            v_errors=1
        try:
            curs.execute("""merge into de2tm.{}_dwh_dim_clients_hist ch
                            using de2tm.{}_stg_clients sc
                            on (ch.client_id=sc.client_id and ch.effective_from<coalesce(sc.update_dt, to_date('1900-01-01','YYYY-MM-DD')))
                            when matched then
                                update set ch.effective_to=sc.update_dt-1
                                where ch.effective_to=to_date('5999-12-31','YYYY-MM-DD')""".format(v_mysxm,v_mysxm))
        except:
            print('Error merge Cards table')
            v_errors=1
        try:
            curs.execute("""merge into de2tm.{}_dwh_dim_terminals_hist th
                            using de2tm.{}_stg_terminals st
                            on (th.terminal_id=st.terminal_id and th.effective_from<to_date('{}', 'YYYY-MM-DD'))
                            when matched then
                                update set th.effective_to=to_date('{}', 'YYYY-MM-DD')
                                where th.effective_to=to_date('5999-12-31','YYYY-MM-DD')""".format(v_mysxm,v_mysxm,v_nxtday,v_curday))
        except:
            print('Error merge Terminals table')
            v_errors=1
        try:
            curs.execute("""insert into de2tm.{}_dwh_dim_accounts_hist(account_num, valid_to, client, effective_from, deleted_flg)
                                select ah.account_num, ah.valid_to, ah.client, to_date('{}', 'YYYY-MM-DD'), 1
                                from de2tm.{}_dwh_dim_accounts_hist ah
                                     left join de2tm.{}_stg_delete_accounts da on ah.account_num=da.account_num
                                where
                                    da.account_num is null and
                                    ah.effective_to=to_date('5999-12-31', 'YYYY-MM-DD') and
                                    ah.deleted_flg=0""".format(v_mysxm,v_curday,v_mysxm,v_mysxm))
            curs.execute("""update de2tm.{}_dwh_dim_accounts_hist set effective_to=to_date('{}', 'YYYY-MM-DD')
                            where account_num in (select ah.account_num
                                                  from de2tm.{}_dwh_dim_accounts_hist ah
                                                       left join de2tm.{}_stg_delete_accounts da on ah.account_num=da.account_num
                                                  where
                                                     da.account_num is null and
                                                     ah.effective_to=to_date('5999-12-31', 'YYYY-MM-DD') and
                                                     ah.deleted_flg=0) and
                                   effective_to=to_date('5999-12-31', 'YYYY-MM-DD') and
                                   deleted_flg=0""".format(v_mysxm,v_prevday,v_mysxm,v_mysxm))
        except:
            print('Error insert delete records in Accounts table')
            v_errors=1
        try:
            curs.execute("""insert into de2tm.{}_dwh_dim_cards_hist(card_num, account_num, effective_from, deleted_flg)
                                select trim(ch.card_num), ch.account_num, to_date('{}', 'YYYY-MM-DD'), 1
                                from de2tm.{}_dwh_dim_cards_hist ch
                                     left join de2tm.{}_stg_delete_cards dc on trim(ch.card_num)=trim(dc.card_num)
                                where
                                    dc.card_num is null and
                                    ch.effective_to=to_date('5999-12-31', 'YYYY-MM-DD') and
                                    ch.deleted_flg=0""".format(v_mysxm,v_curday,v_mysxm,v_mysxm))

            curs.execute("""update de2tm.{}_dwh_dim_cards_hist set effective_to=to_date('{}', 'YYYY-MM-DD')
                            where trim(card_num) in (select trim(ch.card_num)
                                                     from de2tm.{}_dwh_dim_cards_hist ch
                                                     left join de2tm.{}_stg_delete_cards dc on trim(ch.card_num)=trim(dc.card_num)
                                                     where
                                                        dc.card_num is null and
                                                        ch.effective_to=to_date('5999-12-31', 'YYYY-MM-DD') and
                                                        ch.deleted_flg=0) and
                                   effective_to=to_date('5999-12-31', 'YYYY-MM-DD') and
                                   deleted_flg=0""".format(v_mysxm,v_prevday,v_mysxm,v_mysxm))
        except:
            print('Error insert delete records in Cards table')
            v_errors=1
        try:
            curs.execute("""insert into de2tm.{}_dwh_dim_clients_hist(client_id, last_name, first_name, patronymic, date_of_birth, 
                                                                      passport_num, passport_valid_to, phone, effective_from, deleted_flg)
                                select ch.client_id, ch.last_name, ch.first_name, ch.patronymic, ch.date_of_birth,
                                       ch.passport_num, ch.passport_valid_to, ch.phone, to_date('{}', 'YYYY-MM-DD'), 1
                                from de2tm.{}_dwh_dim_clients_hist ch
                                     left join de2tm.{}_stg_delete_clients dc on ch.client_id=dc.client_id
                                where
                                    dc.client_id is null and
                                    ch.effective_to=to_date('5999-12-31', 'YYYY-MM-DD') and
                                    ch.deleted_flg=0""".format(v_mysxm,v_curday,v_mysxm,v_mysxm))

            curs.execute("""update de2tm.{}_dwh_dim_clients_hist set effective_to=to_date('{}', 'YYYY-MM-DD')
                            where client_id in (select ch.client_id
                                                from de2tm.{}_dwh_dim_clients_hist ch
                                                     left join de2tm.{}_stg_delete_clients dc on ch.client_id=dc.client_id
                                                where
                                                    dc.client_id is null and
                                                    ch.effective_to=to_date('5999-12-31', 'YYYY-MM-DD') and
                                                    ch.deleted_flg=0) and
                                  effective_to=to_date('5999-12-31', 'YYYY-MM-DD') and
                                  deleted_flg=0""".format(v_mysxm,v_prevday,v_mysxm,v_mysxm))
        except:
            print('Error insert delete records in Clients table')
            v_errors=1
        try:
            curs.execute("""insert into de2tm.{}_dwh_dim_terminals_hist(terminal_id, terminal_type, terminal_city, terminal_address, effective_from, deleted_flg)
                                select th.terminal_id, th.terminal_type, th.terminal_city, th.terminal_address, to_date('{}', 'YYYY-MM-DD'), 1
                                from de2tm.{}_dwh_dim_terminals_hist th
                                     left join de2tm.{}_stg_terminals st on th.terminal_id=st.terminal_id
                                where
                                    st.terminal_id is null and
                                    th.effective_to=to_date('5999-12-31', 'YYYY-MM-DD') and
                                    th.deleted_flg=0""".format(v_mysxm,v_curday,v_mysxm,v_mysxm))

            curs.execute("""update de2tm.{}_dwh_dim_terminals_hist set effective_to=to_date('{}', 'YYYY-MM-DD')
                            where
                                terminal_id in (select th.terminal_id
                                                from de2tm.{}_dwh_dim_terminals_hist th
                                                     left join de2tm.{}_stg_terminals st on th.terminal_id=st.terminal_id
                                                where
                                                    st.terminal_id is null and
                                                    th.effective_to=to_date('5999-12-31', 'YYYY-MM-DD') and
                                                    th.deleted_flg=0) and
                                effective_to=to_date('5999-12-31', 'YYYY-MM-DD') and
                                deleted_flg=0""".format(v_mysxm,v_prevday,v_mysxm,v_mysxm))
        except:
            print('Error insert delete records in Terminals table')
            v_errors=1
        try:
            #terminals, transaction - факт, по сути мета можно и не делать...
            curs.execute("""update de2tm.{}_meta_pssprt_blcklst
                            set last_update_dt=(select max(entry_dt) from de2tm.{}_stg_pssprt_blcklst)
                            where (select max(entry_dt) from de2tm.{}_stg_pssprt_blcklst) is not null""".format(v_mysxm,v_mysxm,v_mysxm))
            curs.execute("""update de2tm.{}_meta_transactions
                            set last_update_dt=(select max(trans_date) from de2tm.{}_stg_transactions)
                            where (select max(trans_date) from de2tm.{}_stg_transactions) is not null""".format(v_mysxm,v_mysxm,v_mysxm))
            curs.execute("""update de2tm.{}_meta_cards
                            set last_update_dt=(select max(coalesce(update_dt, create_dt)) from de2tm.{}_stg_cards)
                            where (select max(coalesce(update_dt, create_dt)) from de2tm.{}_stg_cards) is not null""".format(v_mysxm,v_mysxm,v_mysxm))
            curs.execute("""update de2tm.{}_meta_accounts
                            set last_update_dt=(select max(coalesce(update_dt, create_dt)) from de2tm.{}_stg_accounts)
                            where (select max(coalesce(update_dt, create_dt)) from de2tm.{}_stg_accounts) is not null""".format(v_mysxm,v_mysxm,v_mysxm))
            curs.execute("""update de2tm.{}_meta_clients
                            set last_update_dt=(select max(coalesce(update_dt, create_dt)) from de2tm.{}_stg_clients)
                            where (select max(coalesce(update_dt, create_dt)) from de2tm.{}_stg_clients) is not null""".format(v_mysxm,v_mysxm,v_mysxm))
            curs.execute("""update de2tm.{}_meta_terminals
                             set last_update_dt=to_date('{}', 'YYYY-MM-DD')""".format(v_mysxm,v_nxtday))
        except:
            print('Error update meta')
            v_errors=1
        try:
            curs.execute("""insert into de2tm.{}_rep_fraud (event_dt, passport, fio, phone, event_type, report_dt)	
                                select event_dt, passport, fio, phone, event_type, report_dt
                                from
                                    (select ft.trans_date as event_dt, clh.passport_num as passport, clh.last_name||' '||clh.first_name||' '||clh.patronymic as fio,clh.phone as phone,
                                            case
                                            when clh.passport_num in (select passport_num from de2tm.{}_dwh_fact_pssprt_blcklst) or
                                                 coalesce(clh.passport_valid_to, to_date('5999-12-31', 'YYYY-MM-DD'))<to_date('{}', 'YYYY-MM-DD')
                                                 then 'Совершение операции при просроченном или заблокированном паспорте (1)'
                                            when ah.valid_to<to_date('{}', 'YYYY-MM-DD')
                                                 then 'Совершение операции при недействующем договоре (2)'
                                            when ft.trans_date in (select max(ftt.trans_date)
                                                    from de2tm.{}_dwh_fact_transactions ftt
                                                    inner join de2tm.{}_dwh_dim_terminals_hist tht on ftt.terminal=tht.terminal_id
                                                    inner join de2tm.{}_dwh_fact_transactions ftt2 on ftt.card_num=ftt2.card_num and ftt.trans_date<ftt2.trans_date
                                                    inner join de2tm.{}_dwh_dim_terminals_hist tht2 on ftt2.terminal=tht2.terminal_id
                                                    where
                                                        ftt.card_num=ftt2.card_num and
                                                        tht.terminal_city!=tht2.terminal_city and
                                                        (ftt2.trans_date-ftt.trans_date)<interval '1' hour)
                                                 then 'Совершение операций в разных городах в течение одного часа (3)'
                                            when ft.trans_date in (select max(ftt.trans_date)
                                                    from de2tm.{}_dwh_fact_transactions ftt
                                                    inner join de2tm.{}_dwh_fact_transactions ftt2 on ftt.card_num=ftt2.card_num and ftt.trans_date<ftt2.trans_date
                                                    inner join de2tm.{}_dwh_fact_transactions ftt3 on ftt2.card_num=ftt3.card_num and ftt2.trans_date<ftt3.trans_date
                                                    where
                                                        ftt.oper_result='REJECT' and ftt2.oper_result='REJECT' and ftt3.oper_result='SUCCESS' and
                                                        ftt.amt>ftt2.amt and ftt2.amt>ftt3.amt and
                                                        (ftt3.trans_date-ftt.trans_date)<interval '20' minute)
                                            then 'Попытка подбора суммы (4)'
                                            else '+' end as event_type,
                                        to_date('{}', 'YYYY-MM-DD') as report_dt
                                    from de2tm.{}_dwh_fact_transactions ft
                                         left join de2tm.{}_dwh_dim_cards_hist ch on trim(ft.card_num)=trim(ch.card_num)
                                         left join de2tm.{}_dwh_dim_accounts_hist ah on ch.account_num=ah.account_num
                                         left join de2tm.{}_dwh_dim_clients_hist clh on ah.client=clh.client_id
                                    where
                                        ft.trans_date>to_date('{}', 'YYYY-MM-DD HH24:MI:SS'))
                                where
                                    event_type!='+' 
                                order by
                                    event_dt desc""".format(v_mysxm,v_mysxm,v_curday,v_curday,v_mysxm,v_mysxm,v_mysxm,v_mysxm,v_mysxm,v_mysxm,v_mysxm,v_curday,v_mysxm,v_mysxm,v_mysxm,v_mysxm,v_curday))
        except:
            print('Error generate report')
            v_errors=1

        #Этап 3.....................................................Подготовка к завершению работы
        conn.commit()                                               #Commit, подтверждение транзакции
        curs.close()                                                #закроем курсор
        conn.close()                                                #завершим соединение
        if v_errors==0:                                             #если все загрузилось, то пеернесем файлы в архив
            #После загрузки соответствующего файла он должен быть переименован в файл с расширением .backup чтобы при следующем запуске файл не искался и перемещен в каталог archive
            os.replace(v_fl_pssp,v_arxdir+'/'+v_fl_pssp+'.backup')
            os.replace(v_fl_term,v_arxdir+'/'+v_fl_term+'.backup')
            os.replace(v_fl_tran,v_arxdir+'/'+v_fl_tran+'.backup')
    if v_daemon==1:                                                 #Если писали в лог, то закроем файл
        flog.close()