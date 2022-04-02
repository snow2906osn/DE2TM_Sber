-- ������� 3

-- �������� �������
/*
select count(employee_id) from hr.employees; --107
select * from hr.employees;
select * from DE_COMMON.TFES_DATASOURCE;
*/

--1.+
/*�������� ������ ����������� (�������, ��� ����������) � �� ����������� (�������, ���) (HR.EMPLOYEES);*/

--select es.employee_id,es.first_name,es.last_name,es.manager_id,em.employee_id,em.first_name,em.last_name
select es.last_name s_fam, es.first_name s_im,em.last_name mng_fam,em.first_name mng_im
from hr.employees es left join hr.employees em on es.manager_id=em.employee_id
order by nvl(es.manager_id,0),es.employee_id;

--2.+
/*�������� ������, ������� ��������� ����� ���������� ����� �� DE_COMMON.TFES_DATASOURCE (� ������� � ������������ ����������
  ������� ��� �� �������� ���������� ����������� ������), ���� ����� ������������, �� ������� '������������ �����' [������ �
  �������������� ���������� ���������];
 */

/* ���������� ����������� ������ - ������ ���� �����-�����@�����N������.�����1������ -> login@domen2.domen1, login@domen3.domen2.domen1
   ������ ������ ������� 1 ����
   ������ @ - "������������ at" �� ����� ���� 1 �������� ������
   ����� ����� ���� ������ 1 ������ � �������� ���� 1 ��������� ������
   ������ ���������� ����� ����� �������
 */

select case when regexp_instr(td.email,'^\w+@\w+[.]')=0 then '������������ �����' else
       regexp_substr(td.email,'^\w+@\w+[.]\w\S+') end good_email,/*����� ����������� email*/
       td.email, td. first_name, td.last_name, td.gender
from DE_COMMON.TFES_DATASOURCE td;

