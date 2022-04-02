--2.+
/*2. Возьмите в работу таблицу:
  По логу событий составьте SCD2 таблицу.
  В ответ присылайте заполненную копию исходной таблицы в excel.
 */

with
  log_table as (
    select '10.01.2021' l_dt,'Поступила книга "Властелин колец", присвоен номер AB843, отправлена на этаж 8, полку 921, назначена цена 320 рублей.' l_evn  from dual
    union
    select '15.01.2021' l_dt,'Поступила книга "Дюна", присвоен номер AF093, отправлена на этаж 4, полку 89, назначена цена 400 рублей.' l_evn  from dual
    union
    select '23.01.2021' l_dt,'Поступила книга "Космос", присвоен номер QA211, отправлена на этаж 1, полку 18, назначена цена 250 рублей.' l_evn  from dual
    union
    select '05.02.2021' l_dt,'Книга "Дюна" перемещена на 7 этаж, полку 49.' l_evn  from dual
    union
    select '20.02.2021' l_dt,'На книгу "Космос" изменилась цена на 300 рублей.' l_evn  from dual
    union
    select '24.02.2021' l_dt,'Поступила книга "Хоббит", присвоен номер FD133, отправлена на этаж 1, полку 12, назначена цена 180 рублей.' l_evn  from dual
    union
    select '07.03.2021' l_dt,'Книгу "Властелин колец" переместили на 1 этаж, полку 8, при этом изменилась цена на 340 рублей.' l_evn  from dual
    union
    select '22.03.2021' l_dt,'Всем книгам подняли цену на 20%.' l_evn  from dual
    union
    select '27.03.2021' l_dt,'Списана книга "Дюна".' l_evn  from dual),
  tmp_parse as (
    select
       lt.l_dt,
       case
           when regexp_substr(lt.l_evn,'^\w+\s\w+\s"')='Поступила книга "' then
                'NEW'||'|'||
                trim(both '"' from regexp_substr(lt.l_evn,'"[а-яА-Я ]+"'))||'|'||
                regexp_substr(lt.l_evn,'[A-Z0-9]+')||'|'||
                trim(trim(leading 'ж' from regexp_substr(lt.l_evn,'[ж]\s\d')))||'|'||
                trim(trim(leading 'у' from regexp_substr(lt.l_evn,'[у]\s\d+')))||'|'||
                trim(trim(leading 'а' from regexp_substr(lt.l_evn,'[а]\s\d+')))
           when regexp_substr(lt.l_evn,'"\s\w{5}')='" перем' then
                'MOVE'||'|'||
                trim(both '"' from regexp_substr(lt.l_evn,'"[а-яА-Я ]+"'))||'| |'||
                trim(trim(trailing 'э' from regexp_substr(lt.l_evn,'\d\s[э]')))||'|'||
                trim(trim(leading 'у' from regexp_substr(lt.l_evn,'[у]\s\d+')))||'|'||
                trim(trim(trailing 'р' from regexp_substr(lt.l_evn,'\d+\s[р]')))
           when regexp_substr(lt.l_evn,'"\s\w{5}')='" измен' then
                'CHANGE'||'|'||
                trim(both '"' from regexp_substr(lt.l_evn,'"[а-яА-Я ]+"'))||'| | | |'||
                trim(trim(trailing 'р' from regexp_substr(lt.l_evn,'\d+\s[р]')))
           when regexp_substr(lt.l_evn,'^\w{6}')='Списан' then
                'DEL'||'|'||
                trim(both '"' from regexp_substr(lt.l_evn,'"[а-яА-Я ]+"'))||'| |0|0|0'
           when regexp_substr(lt.l_evn,'^\w{4}')='Всем' then
                'RISE'||'| | | | |'||
                trim(trim(trailing '%' from regexp_substr(lt.l_evn,'\d+[%]')))
        end csv
   from log_table lt),
  pre_proc as (
    select REGEXP_SUBSTR(csv, '[^|]+', 1, 1) flag,
       to_date(tp.l_dt,'DD.MM.YYYY') l_dt,
       trim(REGEXP_SUBSTR(csv, '[^|]+', 1, 3)) id,
       trim(REGEXP_SUBSTR(csv, '[^|]+', 1, 2)) name,
       trim(REGEXP_SUBSTR(csv, '[^|]+', 1, 4)) stage,
       trim(REGEXP_SUBSTR(csv, '[^|]+', 1, 5)) shelf,
       trim(REGEXP_SUBSTR(csv, '[^|]+', 1, 6)) price
    from tmp_parse tp),
  in_proc as (
    select pp.flag,
       pp.l_dt,
       pp.id,
       pp.name,
       pp.stage,
       pp.shelf,
       cast(pp.price as integer) price
    from pre_proc pp
    where pp.flag!='RISE'
    union all
    select pp.flag,
       pp.l_dt,
       ppm.id,
       ppm.name,
       null stage,
       null shelf,
       (100+pp.price)/100 price
    from pre_proc pp cross join pre_proc ppm
    where pp.flag='RISE' and ppm.flag='NEW')
select first_value(ip.id) over (partition by ip.name order by ip.l_dt asc) id,
       ip.name,
       coalesce(ip.stage,
         case when ip.stage is null then
           lag(ip.stage) over (partition by ip.name order by ip.l_dt asc) else ip.stage end,
         case when ip.stage is null then
           first_value(ip.stage) over (partition by ip.name order by ip.l_dt asc) else ip.stage end) stage,
       coalesce(ip.shelf,
         case when ip.shelf is null then
           lag(ip.shelf) over (partition by ip.name order by ip.l_dt asc) else ip.shelf end,
         case when ip.shelf is null then
           first_value(ip.shelf) over (partition by ip.name order by ip.l_dt asc) else ip.shelf end) shelf,
       case when ip.flag!='RISE' then
         coalesce(ip.price,
           case when ip.price is null and ip.flag!='RISE' then
             lag(ip.price) over (partition by ip.name order by ip.l_dt asc) else ip.price end) else
         coalesce(lag(ip.price) over (partition by ip.name order by ip.l_dt asc)*ip.price,
                  first_value(ip.price) over (partition by ip.name order by ip.l_dt asc)*ip.price) end price,
       to_char(ip.l_dt,'DD.MM.YYYY HH24:MI:SS') valid_from_dt,
       coalesce(to_char(lead(ip.l_dt) over (partition by ip.name order by ip.l_dt asc)-1/24/60/60,'DD.MM.YYYY HH24:MI:SS'),
                to_char(to_timestamp('31.12.5999 00:00:00','DD.MM.YYYY HH24:MI:SS'),'DD.MM.YYYY HH24:MI:SS')) valid_to_dt
from in_proc ip
order by ip.name, ip.l_dt asc;
