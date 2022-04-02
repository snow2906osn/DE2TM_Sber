-- ������� 6 - ��������� �� ������+
--!!!
/* ����������:
�������� ������ �� ��������� �������. ��� ����� � ������� Excel:
1. -- 6. ������� ������ effective_to_dt �� ���������� ��������� (val) ��� ������ �������
����-���� ����� - ������� ������ � ������� ������ ����� ������

2. �� ���������� �� ������� ����� ������� (deleted_flg),  � ������������� ������ �����.
*/

/*��� ����� �� ������� �������
-- 6. ������� ������ effective_to_dt �� ���������� ��������� (val) ��� ������ �������
����-���� ����� - ������� ������ � ������� ������ ����� ������

-- 0.2.4
delete from snow_source where 1=1;
insert into snow_source values (1,'A',to_date('01-09-2021','DD-MM-YYYY'));
insert into snow_source values (2,'B',to_date('01-09-2021','DD-MM-YYYY'));
insert into snow_source values (4,'D',to_date('04-09-2021','DD-MM-YYYY'));
commit;

�� ���������� �� ������� ����� ������� (deleted_flg),  � ������������� ������ �����.
*/

/*
 ��� ��������:
    1. � ��������� ���������� ��� ���� SCD2:  !tfes_target
    � effective_from_dttm
    � effective_to_dttm
    � deleted_flg
    2. �������� ������� �� ��������;
    3. ��� ���������� ����������� 1 ������ � ������������� ���������� ������� ������ ������;
    4. ��� �������� ����������� 1 ������ � deleted_flg = 1 (��� �Y�) � effective_to_dttm = ����������� ������������� � ����������� ������ ������.
 */

/* -- �������� ���� ������
drop table snow_source;
drop table snow_stage;
drop table snow_stage_del;
drop table snow_target;
drop table snow_meta;
*/

-- 0. ���������� ������
--
-- 0.1. �������� �������
create table snow_source /*��������*/
(
    id        number,
    val       varchar2(10),
    update_dt timestamp
);
create table snow_stage /*���������*/
(
    id          number,
    val         varchar2(10),
    update_dt   timestamp
);
create table snow_stage_del /*��������� - ��������*/
(
    id        number
);
create table snow_target /*��������*/
(
    id        number,
    val       varchar2(10),
    effective_from_dttm timestamp,
    effective_to_dttm   timestamp,
    deleted_flg         number(1) default(0)
);
create table snow_meta /*����������*/
(
    dbname           varchar2(30),
    tablename        varchar2(100),
    last_update      timestamp(6)
);
-- ������� � ������� �������������� ���� � ������� ������
delete from snow_target where 1=1;
delete from snow_source where 1=1;
delete from snow_meta where 1=1;
insert into snow_meta(dbname,tablename,last_update) values('de2tm','snow_target',to_date('01-01-1900','DD-MM-YYYY'));

-- select * from snow_source;
-- 0.2. ������ ������:
-- 0.2.1
/*
insert into snow_source values (1,'A',to_date('01-09-2021','DD-MM-YYYY'));
insert into snow_source values (2,'B',to_date('01-09-2021','DD-MM-YYYY'));
insert into snow_source values (3,'C',to_date('01-09-2021','DD-MM-YYYY'));
commit;
*/
-- 0.2.2
/* �� �������, �.�. ��� ���������� � �������������� 3 ������� � 1 ����
insert into snow_source values (4,'D',to_date('04-09-2021','DD-MM-YYYY'));
commit;
*/
-- 0.2.3
/*
delete from snow_source where 1=1;
insert into snow_source values (1,'A',to_date('01-09-2021','DD-MM-YYYY'));
insert into snow_source values (2,'B',to_date('01-09-2021','DD-MM-YYYY'));
insert into snow_source values (3,'Z',to_date('05-09-2021','DD-MM-YYYY'));
insert into snow_source values (4,'D',to_date('04-09-2021','DD-MM-YYYY'));
commit;
*/
-- 0.2.4
/*
delete from snow_source where 1=1;
insert into snow_source values (2,'B',to_date('01-09-2021','DD-MM-YYYY'));
insert into snow_source values (3,'Z',to_date('05-09-2021','DD-MM-YYYY'));
insert into snow_source values (4,'D',to_date('04-09-2021','DD-MM-YYYY'));
commit;
*/
-- 0.2.5 ����-���� ����� - ������� ������ � ������� ������ ����� ������. ������ 3 ������ (Z)
/*
delete from snow_source where 1=1;
insert into snow_source values (2,'B',to_date('01-09-2021','DD-MM-YYYY'));
insert into snow_source values (4,'D',to_date('04-09-2021','DD-MM-YYYY'));
commit;
*/

-- �������� SCD2
--
-- 1. ���������� ��������, ������� stage
delete from snow_stage where 1=1;
delete from snow_stage_del where 1=1;

-- 2. ������ ������ �� ��������� � stage
-- ��� update � insert
-- select * from snow_stage ss;
insert into snow_stage(id,val,update_dt)
  select ss.id, ss.val, ss.update_dt
  from snow_source ss
  where
    -- � �������� CDC_SCD1 ���������� �� null ��� last_update ������������ � ������������� ��� ������ �������� �� ��������� �� ��������.
    coalesce(ss.update_dt,current_timestamp)>coalesce((select sm.last_update from snow_meta sm where sm.dbname='de2tm' and sm.tablename='snow_target'),to_date('01-01-1900','DD-MM-YYYY'));

-- 3. ��� ����������("��� �������� ����������� 1 ������ � deleted_flg = 1") ���������� � ������� ������ - ��������
-- select * from snow_stage_del sd;
insert into snow_stage_del
  select ss.id
  from snow_source ss;

-- 4. ������� ���������� �� ��������� ������� ("��� �������� ����������� 1 ������ � deleted_flg = 1") ���������� � ������� ������ - ��������
-- "�� ���������� �� ������� ����� ������� (deleted_flg), � ������������� ������ �����", �� ���������� ������ ������� ��� ������ � ����� ����� ������, � target � stage ���������
-- �.�. target ����� ���� �������, �� ��������� ��������  ����� ���������� �������� stage: ��� �������� ����� ���������� ������ ��������. ������� ������, �������� � stage � �� stage �������� � target
-- select * from snow_stage ss;
insert into snow_stage(id,val,update_dt)
  select st.id, st.val, null
  from snow_target st left join snow_stage_del sd on st.id=sd.id
  where
    st.effective_to_dttm=to_timestamp('31-12-5999','DD-MM-YYYY') and
    sd.id is null and not exists (select 1 from snow_target stt where stt.deleted_flg=1 and stt.id=st.id);
-- select * from snow_target ss;
insert into snow_target(id,val,effective_from_dttm,effective_to_dttm,deleted_flg)
  select ss.id, ss.val, to_timestamp(sysdate),to_timestamp('31-12-5999','DD-MM-YYYY'),1
  from snow_stage ss
  where
    ss.update_dt is null;

-- 5. ��������� ������ � ���������
-- select * from snow_target st;
merge into snow_target st
using snow_stage ss on (st.id=ss.id and st.val=ss.val)
when matched then
  update set st.effective_to_dttm=to_timestamp(sysdate)-1/24/60/60 where effective_from_dttm!=to_timestamp(sysdate)
when not matched then
  insert (id,val,effective_from_dttm,effective_to_dttm) values(ss.id,ss.val,ss.update_dt,to_timestamp('31-12-5999','DD-MM-YYYY'));

-- 6. ������� ������ effective_to_dt �� ���������� ��������� (val) ��� ������ �������
-- "����-���� ����� - ������� ������ � ������� ������ ����� ������" ������� ������, �������� ��.
-- select * from snow_target st;
update snow_target st
set
  st.effective_to_dttm=(select max(ss.update_dt)-1/24/60/60 from snow_stage ss where st.id=ss.id)
where
  st.effective_to_dttm=to_timestamp('31-12-5999','DD-MM-YYYY') and
  st.effective_from_dttm<(select max(ss.update_dt) from snow_stage ss where st.id=ss.id);

-- 7. ��������� ���������� - ���� ������������ ���������
-- select * from snow_meta sm;
update snow_meta sm
set sm.last_update=(select max(coalesce(st.update_dt,to_timestamp(sysdate))) from snow_stage st)
where
  sm.dbname='de2tm' and
  sm.tablename='snow_target';

/* �������� ������
select * from snow_source ss;
select * from snow_stage ss;
select * from snow_target st order by st.id, st.effective_to_dttm;
select * from snow_meta sm;
*/

-- 8. ����������� ����������
commit;
