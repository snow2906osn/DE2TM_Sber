-- ������� 2

-- �������� �������
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
/*��� ������� ������� ��������� ���-�� � ����� ���������� � ������
  (��� �������� �� ������ ����� � ����� ������� ������������ ������� HW2_CURRENCY_EXCHANGE ����������� join);*/

--!!! ����� ���� ������� ������������� (view) ������ with, �� ���� �� ����� �� ����������...
--������� 1
select c.id, c.LASTNAME, c.NAME, c.CITY, count(t.CLIENT_ID) t_cnt, sum(nvl(t.MONEY_AMOUNT*ce.coef,0)) t_sum
from
    HW2_CLIENT c left join HW2_TRANSACTIONS t on c.ID = t.CLIENT_ID
    left join HW2_CURRENCY_EXCHANGE ce on t.CURRENCY_ID = ce.id and
                                          exists (select 1 from HW2_CURRENCY_TYPES ct where ce.TO_CURRENCY_ID = ct.id and ct.TITLE='�����')
group by c.id, c.LASTNAME, c.NAME, c.CITY
order by c.id;

--������� 2 (����� ����������� ���...)
with T_CURRENCY_EXCHANGE as (
    select ce.ID t_fr, ct.ID t_to, ct.TITLE t_tonm, ce.COEF coef
    from
        HW2_CURRENCY_EXCHANGE ce inner join HW2_CURRENCY_TYPES ct on ce.TO_CURRENCY_ID = ct.id
)
select c.id, c.LASTNAME, c.NAME, c.CITY, count(t.CLIENT_ID) t_cnt, sum(nvl(t.MONEY_AMOUNT*tce.coef,0)) t_sum
from
    HW2_CLIENT c left join HW2_TRANSACTIONS t on c.ID = t.CLIENT_ID
    left join T_CURRENCY_EXCHANGE tce on t.CURRENCY_ID = tce.t_fr and tce.t_tonm='�����'
group by c.id, c.LASTNAME, c.NAME, c.CITY
order by c.id;

--2+
/*��� ������� ������� ������� ��� email � �������.
  ���� ������� �� ������, �� � ���� ���������� ���������� ���� �������;*/
select c.id,c.LASTNAME,c.NAME, c.CITY, trim(nvl(l.EMAIL,'��� ������')) eml, nvl(l.PHONE,'��� ������') tel
from
    HW2_CLIENT c left join HW2_LOCATORS l on c.LOCATOR_ID = l.LOCATOR_ID
order by c.id;

--���������� � �������� �������: ����������� ����������:
with T_CURRENCY_EXCHANGE as (
    select ce.ID t_fr, ct.ID t_to, ct.TITLE t_tonm, ce.COEF coef
    from
        HW2_CURRENCY_EXCHANGE ce inner join HW2_CURRENCY_TYPES ct on ce.TO_CURRENCY_ID = ct.id
)
select c.id, c.LASTNAME, c.NAME, c.CITY, trim(nvl(l.EMAIL,'��� ������')) eml, nvl(l.PHONE,'��� ������') tel, count(t.CLIENT_ID) t_cnt, sum(nvl(t.MONEY_AMOUNT*tce.coef,0)) t_sum
from
    HW2_CLIENT c left join HW2_TRANSACTIONS t on c.ID = t.CLIENT_ID
    left join T_CURRENCY_EXCHANGE tce on t.CURRENCY_ID = tce.t_fr and tce.t_tonm='�����'
    left join HW2_LOCATORS l on c.LOCATOR_ID = l.LOCATOR_ID
group by c.id, c.LOCATOR_ID, c.LASTNAME, c.NAME, c.CITY, l.EMAIL, l.PHONE
order by c.id;

-- 3+
/*��� ������� ������� ���������� ���������� ���� ������� ���� �� ������ ��������:
  ���� ���� �� ���� ������� ������, �� ���� ��������� �������� �Y�, ����� �N�*/
--exists ���������� �� 1 ���������...�.�. ������� ��� count
select c.id,c.LASTNAME,c.NAME,
       nvl((select 'Y' from dual where exists (select 1 from HW2_LOCATORS l2 where l2.LOCATOR_ID=c.LOCATOR_ID and coalesce(l2.PHONE,l2.EMAIL) is not null)),'N') flgc
from
    HW2_CLIENT c
order by c.id;

--���������� � �������� �������: ����������� ������ ���-�� ���������:
select T.ID, T.LASTNAME, T.NAME,
       nvl((select 'Y' from dual where exists (select 1 from HW2_LOCATORS l2 where l2.LOCATOR_ID=T.LOCATOR_ID and coalesce(l2.PHONE,l2.EMAIL) is not null)),'N') flgc,
       T.eml, T.tel, T.t_cnt, T.t_sum
from (with T_CURRENCY_EXCHANGE as (
        select ce.ID t_fr, ct.ID t_to, ct.TITLE t_tonm, ce.COEF coef
        from
            HW2_CURRENCY_EXCHANGE ce inner join HW2_CURRENCY_TYPES ct on ce.TO_CURRENCY_ID = ct.id
)
select c.id, c.LOCATOR_ID, c.LASTNAME, c.NAME, trim(nvl(l.EMAIL,'��� ������')) eml, nvl(l.PHONE,'��� ������') tel, count(t.CLIENT_ID) t_cnt, sum(nvl(t.MONEY_AMOUNT*tce.coef,0)) t_sum
from
    HW2_CLIENT c left join HW2_TRANSACTIONS t on c.ID = t.CLIENT_ID
    left join T_CURRENCY_EXCHANGE tce on t.CURRENCY_ID = tce.t_fr and tce.t_tonm='�����'
    left join HW2_LOCATORS l on c.LOCATOR_ID = l.LOCATOR_ID
group by c.id, c.LOCATOR_ID, c.LASTNAME, c.NAME, l.EMAIL, l.PHONE
order by c.id) T;

--4+
/*��� ������� ������� ���������� ���� ����-�� ���������� ����� 5�:
  ���� ���� ��������� ����� 5 ����������, �� ���� ��������� �������� �Y�, ����� �N�;*/
select c.id,c.LASTNAME,c.NAME,
       case when count(t.CLIENT_ID) > 5 then 'Y' else 'N' end flgt
from
    HW2_CLIENT c left join  HW2_TRANSACTIONS t on c.ID = t.CLIENT_ID
group by c.id,c.LASTNAME,c.NAME
order by c.id;

--���������� � �������� �������: ����������� ������ ���-�� ����������:
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
select c.id, c.LOCATOR_ID, c.LASTNAME, c.NAME, trim(nvl(l.EMAIL,'��� ������')) eml, nvl(l.PHONE,'��� ������') tel, count(t.CLIENT_ID) t_cnt, sum(nvl(t.MONEY_AMOUNT*tce.coef,0)) t_sum
from
    HW2_CLIENT c left join HW2_TRANSACTIONS t on c.ID = t.CLIENT_ID
    left join T_CURRENCY_EXCHANGE tce on t.CURRENCY_ID = tce.t_fr and tce.t_tonm='�����'
    left join HW2_LOCATORS l on c.LOCATOR_ID = l.LOCATOR_ID
group by c.id, c.LOCATOR_ID, c.LASTNAME, c.NAME, l.EMAIL, l.PHONE
order by c.id) T;

--5+
/*��� �������� ����������� ������ ��������, ��� ������� ������ ���� �� 1 �������
  � ����������� ����� 5 ����������. � �������� ������ ����������� ��������� ����:*/

--!!! ������������� (view) ��� �������, ��� � ����������������� �� �����������. �� ���� ��� ���� �� ��������� ������� ���� �� ������...
--!!! � ��� ����� ���� create or replace view SNOW_TASK5 as ... ��� materialized view ...

select T.ID, T.LASTNAME, T.NAME, T.tel, T.eml, T.t_sum
from (with T_CURRENCY_EXCHANGE as (
        select ce.ID t_fr, ct.ID t_to, ct.TITLE t_tonm, ce.COEF coef
        from
            HW2_CURRENCY_EXCHANGE ce inner join HW2_CURRENCY_TYPES ct on ce.TO_CURRENCY_ID = ct.id
)
select c.id, c.LOCATOR_ID, c.LASTNAME, c.NAME, trim(nvl(l.EMAIL,'��� ������')) eml, nvl(l.PHONE,'��� ������') tel, count(t.CLIENT_ID) t_cnt, sum(nvl(t.MONEY_AMOUNT*tce.coef,0)) t_sum
from
    HW2_CLIENT c left join HW2_TRANSACTIONS t on c.ID = t.CLIENT_ID
    left join T_CURRENCY_EXCHANGE tce on t.CURRENCY_ID = tce.t_fr and tce.t_tonm='�����'
    left join HW2_LOCATORS l on c.LOCATOR_ID = l.LOCATOR_ID
group by c.id, c.LOCATOR_ID, c.LASTNAME, c.NAME, l.EMAIL, l.PHONE
order by c.id) T
where
    exists (select 1 from HW2_LOCATORS l2 where l2.LOCATOR_ID=T.LOCATOR_ID and coalesce(l2.PHONE,l2.EMAIL) is not null) and
    T.t_cnt > 5
order by T.id;
