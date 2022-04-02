-- ������� 4

-- �������� �������
/*
select count(client_id), count(distinct client_id), count(distinct client_id)*4 from de_common.HW4_FCT_TRANSACTION; --!!! 1015, 8 � 32 (������ ����)
select * from de_common.HW4_FCT_TRANSACTION ft order by ft.client_id, ft.trans_dt;

*/

--1.+
/*�� ������ ������� de_common.HW4_FCT_TRANSACTION (�������� ����������, ����������� � 2021 ����)
  ��� ������� ������� � ������� �������� ���������� ��������� ��������� ��������:
 */
with MOD_HW4_FCT_TRANSACTION as (
select ft.client_id,
       floor((extract(month from ft.trans_dt)+2)/3) quarter,
       ft.trans_dt,
       ft.trans_amt
from de_common.HW4_FCT_TRANSACTION ft) -- CTE ��� ���������� ���-�� ��������� � �������� ������ ��������... 1 ��� ��������� � ���������� ��������
select distinct mft.client_id,
       mft.quarter,
       last_value(mft.trans_amt) over
         (partition by mft.client_id, mft.quarter order by mft.trans_dt range between unbounded preceding and unbounded following) sumq_last, -- ����� ��������� ���������� �� ������� �� �������
       count(mft.trans_amt) over
         (partition by mft.client_id, mft.quarter) cntq, -- ���-�� ���������� �� ������� �� �������
       max(mft.trans_amt) over
         (partition by mft.client_id, mft.quarter) sumq_max, -- ����� ������������ ���������� �� ������� �� �������
       round(avg(mft.trans_amt) over
         (partition by mft.client_id, mft.quarter),3) sumq_avg -- ����� ������� ���������� �� ������� �� �������
from MOD_HW4_FCT_TRANSACTION mft
order by mft.client_id, mft.quarter;

--2.+
/*�� ������ ������� de_common.HW4_FCT_TRANSACTION ��� ������� ������� � ������� ������ �������
 */
with MOD_HW4_FCT_TRANSACTION as (
select ft.client_id,
       trunc(ft.trans_dt,'MM') month,
       sum(ft.trans_amt) trans_amt
from de_common.HW4_FCT_TRANSACTION ft
group by ft.client_id,trunc(ft.trans_dt,'MM'))
select mft.client_id,
       mft.month,
       mft.trans_amt summ, -- ����� ���� ���������� � ������� ������
       /*sum(mft.trans_amt) over
         (partition by mft.client_id,mft.month) summ-noneed, -- ����� ���� ���������� � ������� ������*/
       nvl(lag(mft.trans_amt) over
         (partition by mft.client_id order by mft.month),0) sumprevm, -- ����� ���� ���������� � ���������� ������
       nvl(lead(mft.trans_amt) over
         (partition by mft.client_id order by mft.month),0) sumnextm, -- ����� ���� ���������� � ��������� ������
       sum(mft.trans_amt) over
         (partition by mft.client_id order by mft.month rows between unbounded preceding and current row) sumaddit, -- ���������� ����� ���� ���������� � ������ ����
       sum(mft.trans_amt) over
         (partition by mft.client_id order by mft.month rows between 2 preceding and 1 following) sumwind -- ����� ���������� � ���������� ���� [-2 ������; +1 �����] ������������ ��������� ������
from MOD_HW4_FCT_TRANSACTION mft
order by mft.client_id, mft.month;

--3.+
/*�� ������ ������� de_common.HW4_FCT_TRANSACTION ��� ������� ������� � ������ ������ ������� ������������� ����������.
 */

/*� �������� ������� ����������� ���������� ���� � ������� ���������� ���� (DD-MM-YYYY HH:MM:SS),
  ��� ����� ����������� ������� ���������� ��������� dens_rank �� 2 ������
  */
with
     MOD_HW4_FCT_TRANSACTION as (select ft.client_id,
                                        trunc(ft.trans_dt,'MM') month,
                                        ft.trans_dt,
                                        ft.trans_amt
                                 from de_common.HW4_FCT_TRANSACTION ft),
     MOD_HW4_FCT_TRANSACTION_NROWS as (select mft.client_id,
                                              mft.month,
                                              dense_rank() over
                                                (partition by mft.client_id, mft.month order by mft.trans_dt desc) nrow, -- ���������� �� ��������, � �������� ����� ����... 2 �� ����� ������ �������� ������ �������������
                                              mft.trans_dt,
                                              mft.trans_amt
                                       from MOD_HW4_FCT_TRANSACTION mft)
select mftn.client_id,
       mftn.month,
       mftn.trans_amt
from MOD_HW4_FCT_TRANSACTION_NROWS mftn
where
  mftn.nrow=2
order by mftn.client_id, mftn.month;
