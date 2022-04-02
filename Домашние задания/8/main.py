import pandas
import jaydebeapi

if __name__ == '__main__':
    mk_offline=1                                                            #1-создаем оффлайн sql, иное нет
    mk_disable_conn=0                                                       #1-отключить соединение с RDBMS, иное нет
    mk_eol='\r\n'                                                           #перевод строки
    if mk_offline==1:
        mk_sql=open('make_sql.sql','w')                                     #создаем файл

    mk_offline_col=''                                                       #определим переменные
    mk_offline_row=''                                                       #определим переменные

    if mk_disable_conn!=1:
        conn=jaydebeapi.connect('oracle.jdbc.driver.OracleDriver','jdbc:oracle:thin:de2tm/xxxxxxxxxxxx@de-oracle.chronosavant.ru:1521/deoracle',
                                ['de2tm','xxxxxxxxxxxx'],'ojdbc8.jar')
        curs=conn.cursor()

    xls=pandas.read_excel('/home/de2tm/HW8/HW_8_calls.xlsx', sheet_name=None, header=0)#получим dict, размерность 2 (листа excel)
    for sheet in xls.keys():                                                #обход всех ключей(листов excel) dict
        mk_ddl=0                                                            #определим переменные
        mk_table='snow_{}'.format(sheet)                                    #название таблицы
        mk_offline_ddl=[]                                                   #определим переменные DML+DDL
        mk_offline_ddl.append('create table {} ('.format(mk_table))         #создаем DDL, т.к. иногда надо работать с https://livesql.oracle.com/
        mk_offline_col=''                                                   #подготовим следующую строку
        try:
            if mk_disable_conn!=1:
                curs.execute('drop table {}'.format(mk_table))              #удалим таблицу, если есть
        except:
            print('Table {} not exists'.format(mk_table))

        for idx, row in xls[sheet].iterrows():                              #обходим все строки листа и создаем DML
            rowset=row.to_dict()
            for col in rowset:                                              #обходим колонки листа
                if type(rowset[col])==pandas.Timestamp:
                    mk_offline_row=mk_offline_row+'to_timestamp(\''+str(rowset[col])+'\',\'YYYY-MM-DD HH24:MI:SS.FF\'),'
                    if mk_ddl==0:
                        mk_offline_col=mk_offline_col+col+','
                        mk_offline_ddl.append('{} timestamp,'.format(col))
                elif type(rowset[col])==int:
                    mk_offline_row=mk_offline_row+str(rowset[col])+','
                    if mk_ddl==0:
                        mk_offline_col=mk_offline_col+col+','
                        mk_offline_ddl.append('{} integer,'.format(col))
                else:
                    mk_offline_row=mk_offline_row+'\''+str(rowset[col])+'\','
                    if mk_ddl==0:
                        mk_offline_col=mk_offline_col+col+','
                        mk_offline_ddl.append('{} varchar2(255),'.format(col))#грамотно было бы вычислить, но не в рамках лабораторной

            if mk_ddl==0:                                                   #если DDL еще не подготовлен, то формируем
                mk_ddl=1                                                    #создаем по 1-й строке таблицу. размерность полей не проверяем, ставим по максимумум
                mk_ddl_len=len(mk_offline_ddl)
                mk_offline_ddl[mk_ddl_len-1]=mk_offline_ddl[mk_ddl_len-1].rstrip(',')
                mk_offline_col=mk_offline_col.rstrip(',')                   #уберем из блока колонок крайнюю ,
                mk_curs_ddl=''                                              #определим переменную
                for srow in mk_offline_ddl:                                 #соберем DDL
                    mk_curs_ddl=mk_curs_ddl+srow
                    if mk_offline == 1:
                        mk_sql.write(srow+mk_eol)
                mk_curs_ddl=mk_curs_ddl+')'
                if mk_offline==1:
                    mk_sql.write(');'+mk_eol)
                mk_offline_ddl=[]                                           #определим переменные
                try:
                    if mk_disable_conn!=1:
                        curs.execute(mk_curs_ddl)                           #создадим таблицу
                except:
                    print('Fail to create table {}'.format(mk_table))

            mk_offline_row=mk_offline_row.rstrip(',')                       #уберем из блока values() крайнюю ,
            mk_insert_dml='insert into {} ({}) values({})'.format(mk_table,mk_offline_col,mk_offline_row)
            mk_offline_ddl.append(mk_insert_dml)
            mk_offline_row=''                                               #подготовим следующую строку
            try:
                if mk_disable_conn!=1:
                    curs.execute(mk_insert_dml)                             #создадим таблицу
            except:
                print('Insert to table {} fail'.format(mk_table))

        if mk_offline==1:                                                   #сохраним DML
            for srow in mk_offline_ddl:
                mk_sql.write(srow+';'+mk_eol)

    if mk_offline==1:                                                       #если писали в файл, закроем...
        mk_sql.close()

    mk_select="""select mk.emp_id,mk.last_name,mk.first_name, mk.call_cnt from (select md.emp_id,md.last_name,md.first_name, md.call_cnt, dense_rank() over (order by md.call_cnt desc) nrow
                 from (select de.emp_id,de.last_name,de.first_name,count(de.emp_id) call_cnt
                 from snow_dim_employee de inner join snow_fct_communication fc on de.emp_id=fc.emp_id and fc.score>=4
                 group by de.emp_id,de.last_name,de.first_name
                 order by count(de.emp_id) desc) md) mk
                 where mk.nrow<=5 order by mk.nrow asc"""
    res=[]                                                                  #определим переменную
    try:
        if mk_disable_conn!=1:
            curs.execute(mk_select)                                         #выполним запрос
            res=curs.fetchall()
            curs.close()
    except:
        print('Fetch error')
    xlsout=pandas.DataFrame(res, columns=['emp_id','last_name','first_name','call_cnt'])
    #print(xlsout)
    xlsout.to_excel('snow_top_5_employees.xlsx',index=None)

    if mk_disable_conn!=1:
        conn.close()
