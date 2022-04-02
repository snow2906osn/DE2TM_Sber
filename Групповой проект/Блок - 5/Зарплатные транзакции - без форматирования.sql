-- ��������� ������ - ���� 5 "���������� ����������"
/* �������� �������
select * from de_common.group_dict_income_type;

--2021-01-01, 2021-12-01 � 2094
select min(to_date(income_month,'YYYY-MM')) dmin, max(to_date(income_month,'YYYY-MM')) dmax, count(client_id) from de_common.group_fct_income_transactions;

 */

ALTER SESSION SET nls_date_language = 'AMERICAN';

with cte_fct_income_transactions as (
select it.client_id,
       months_between(to_date('2021-12-01','YYYY-MM-DD'), to_date(it.income_month,'YYYY-MM')) diff_m,
       it.income_sum_amt,
       it.transaction_id
from
    de_common.group_fct_income_transactions it
    inner join de_common.group_dict_income_type di on it.income_type=di.income_type
    and (di.income_nm='���������� ����������' and
         to_date('2021-12-01','YYYY-MM-DD')>to_date(it.income_month,'YYYY-MM')))
select
       -- ����� ���������� �� ������ ����� �� �������� ����
       sum(case when cte.diff_m=1 then cte.income_sum_amt else 0 end) SALARY_2M_AMT, /* !!! ������������ �� ��������� ��, �� �� ���� SALARY_1M_AMT*/
       -- ����� ���������� �� ������ ����� �� �������� ����
       sum(case when cte.diff_m=2 then cte.income_sum_amt else 0 end) SALARY_1M_AMT, /* !!! ������������ �� ��������� ��, �� �� ���� SALARY_2M_AMT*/
       -- ����� ���������� �� ������ ����� �� �������� ����
       sum(case when cte.diff_m=3 then cte.income_sum_amt else 0 end) SALARY_3M_AMT,
       -- ����� ���������� �� ��������� ����� �� �������� ����
       sum(case when cte.diff_m=4 then cte.income_sum_amt else 0 end) SALARY_4M_AMT,
       -- ����� ���������� �� ����� ����� �� �������� ����
       sum(case when cte.diff_m=5 then cte.income_sum_amt else 0 end) SALARY_5M_AMT,
       -- ����� ���������� �� ������ ����� �� �������� ����
       sum(case when cte.diff_m=6 then cte.income_sum_amt else 0 end) SALARY_6M_AMT,
       -- ���-�� ���������� �� ������ ����� �� �������� ����
       count(case when cte.diff_m=1 then cte.income_sum_amt else null end) SALARY_1M_CNT,
       -- ���-�� ���������� �� ������ ����� �� �������� ����
       count(case when cte.diff_m=2 then cte.income_sum_amt else null end) SALARY_2M_CNT,
       -- ���-�� ���������� �� ������ ����� �� �������� ����
       count(case when cte.diff_m=3 then cte.income_sum_amt else null end) SALARY_3M_CNT,
       -- ���-�� ���������� �� ��������� ����� �� �������� ����
       count(case when cte.diff_m=4 then cte.income_sum_amt else null end) SALARY_4M_CNT,
       -- ���-�� ���������� �� ����� ����� �� �������� ����
       count(case when cte.diff_m=5 then cte.income_sum_amt else null end) SALARY_5M_CNT,
       -- ���-�� ���������� �� ������ ����� �� �������� ����
       count(case when cte.diff_m=6 then cte.income_sum_amt else null end) SALARY_6M_CNT,
       -- ��������� ����� ���������� ���������� �� ��������� ����� � ����� �� 3 ��������� ������
       round(sum(case when cte.diff_m=1 then cte.income_sum_amt else 0 end)/sum(case when cte.diff_m<=3 then cte.income_sum_amt else 0 end),2) SALARY_1M_TO_3M_AMT_PCT,
       -- ��������� ����� ���������� ���������� �� ��������� ����� � ����� �� 6 ��������� �������
       round(sum(case when cte.diff_m=1 then cte.income_sum_amt else 0 end)/sum(case when cte.diff_m<=6 then cte.income_sum_amt else 0 end),2) SALARY_1M_TO_6M_AMT_PCT,
       -- ��������� ���-�� ���������� ���������� �� ��������� ����� � ���-�� �� 3 ��������� ������
       round(count(case when cte.diff_m=1 then cte.income_sum_amt else null end)/count(case when cte.diff_m<=3 then cte.income_sum_amt else null end),2) SALARY_1M_TO_3M_CNT_PCT,
       -- ��������� ���-�� ���������� �� ��������� ����� � ���-�� �� 6 ��������� �������
       round(count(case when cte.diff_m=1 then cte.income_sum_amt else null end)/count(case when cte.diff_m<=6 then cte.income_sum_amt else null end),2) SALARY_1M_TO_6M_CNT_PCT,
       -- ���-�� �������, � ������� ���� ���������� ����������, �� ��������� 6 �������
       count(distinct case when cte.diff_m<=6 then cte.diff_m else null end) SALARY_DURING_6M_CNT,
       -- ���-�� ������� �����, ����� ���� ��������� ��������� ���������� ����������
       min(diff_m) LAST_SAL_TRANS_MONTH_CNT,
       -- ���-�� ������� �����, ����� ���� ��������� ������ ���������� ����������
       max(diff_m) FIRST_SAL_TRANS_MONTH_CNT
from cte_fct_income_transactions cte
group by cte.client_id
order by cte.client_id;
