--2.+
/*2. �������� � ������ �������:
  �� ���� ������� ��������� SCD2 �������.
  � ����� ���������� ����������� ����� �������� ������� � excel.
 */

with
  log_table as (
    select '10.01.2021' l_dt,'��������� ����� "��������� �����", �������� ����� AB843, ���������� �� ���� 8, ����� 921, ��������� ���� 320 ������.' l_evn  from dual
    union
    select '15.01.2021' l_dt,'��������� ����� "����", �������� ����� AF093, ���������� �� ���� 4, ����� 89, ��������� ���� 400 ������.' l_evn  from dual
    union
    select '23.01.2021' l_dt,'��������� ����� "������", �������� ����� QA211, ���������� �� ���� 1, ����� 18, ��������� ���� 250 ������.' l_evn  from dual
    union
    select '05.02.2021' l_dt,'����� "����" ���������� �� 7 ����, ����� 49.' l_evn  from dual
    union
    select '20.02.2021' l_dt,'�� ����� "������" ���������� ���� �� 300 ������.' l_evn  from dual
    union
    select '24.02.2021' l_dt,'��������� ����� "������", �������� ����� FD133, ���������� �� ���� 1, ����� 12, ��������� ���� 180 ������.' l_evn  from dual
    union
    select '07.03.2021' l_dt,'����� "��������� �����" ����������� �� 1 ����, ����� 8, ��� ���� ���������� ���� �� 340 ������.' l_evn  from dual
    union
    select '22.03.2021' l_dt,'���� ������ ������� ���� �� 20%.' l_evn  from dual
    union
    select '27.03.2021' l_dt,'������� ����� "����".' l_evn  from dual),
  tmp_parse as (
    select
       lt.l_dt,
       case
           when regexp_substr(lt.l_evn,'^\w+\s\w+\s"')='��������� ����� "' then
                'NEW'||'|'||
                trim(both '"' from regexp_substr(lt.l_evn,'"[�-��-� ]+"'))||'|'||
                regexp_substr(lt.l_evn,'[A-Z0-9]+')||'|'||
                trim(trim(leading '�' from regexp_substr(lt.l_evn,'[�]\s\d')))||'|'||
                trim(trim(leading '�' from regexp_substr(lt.l_evn,'[�]\s\d+')))||'|'||
                trim(trim(leading '�' from regexp_substr(lt.l_evn,'[�]\s\d+')))
           when regexp_substr(lt.l_evn,'"\s\w{5}')='" �����' then
                'MOVE'||'|'||
                trim(both '"' from regexp_substr(lt.l_evn,'"[�-��-� ]+"'))||'| |'||
                trim(trim(trailing '�' from regexp_substr(lt.l_evn,'\d\s[�]')))||'|'||
                trim(trim(leading '�' from regexp_substr(lt.l_evn,'[�]\s\d+')))||'|'||
                trim(trim(trailing '�' from regexp_substr(lt.l_evn,'\d+\s[�]')))
           when regexp_substr(lt.l_evn,'"\s\w{5}')='" �����' then
                'CHANGE'||'|'||
                trim(both '"' from regexp_substr(lt.l_evn,'"[�-��-� ]+"'))||'| | | |'||
                trim(trim(trailing '�' from regexp_substr(lt.l_evn,'\d+\s[�]')))
           when regexp_substr(lt.l_evn,'^\w{6}')='������' then
                'DEL'||'|'||
                trim(both '"' from regexp_substr(lt.l_evn,'"[�-��-� ]+"'))||'| |0|0|0'
           when regexp_substr(lt.l_evn,'^\w{4}')='����' then
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
