-- √рупповой проект - блок 5 "«арплатные транзакции"
/* “естова€ выборка
select * from de_common.group_dict_income_type;

--2021-01-01, 2021-12-01 и 2094
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
    and (di.income_nm='«арплатное начисление' and
         to_date('2021-12-01','YYYY-MM-DD')>to_date(it.income_month,'YYYY-MM')))
select
       -- —умма транзакций за первый мес€ц до отчетной даты
       sum(case when cte.diff_m=1 then cte.income_sum_amt else 0 end) SALARY_2M_AMT, /* !!! наименование на основании “«, но по идее SALARY_1M_AMT*/
       -- —умма транзакций за второй мес€ц до отчетной даты
       sum(case when cte.diff_m=2 then cte.income_sum_amt else 0 end) SALARY_1M_AMT, /* !!! наименование на основании “«, но по идее SALARY_2M_AMT*/
       -- —умма транзакций за третий мес€ц до отчетной даты
       sum(case when cte.diff_m=3 then cte.income_sum_amt else 0 end) SALARY_3M_AMT,
       -- —умма транзакций за четвертый мес€ц до отчетной даты
       sum(case when cte.diff_m=4 then cte.income_sum_amt else 0 end) SALARY_4M_AMT,
       -- —умма транзакций за п€тый мес€ц до отчетной даты
       sum(case when cte.diff_m=5 then cte.income_sum_amt else 0 end) SALARY_5M_AMT,
       -- —умма транзакций за шестой мес€ц до отчетной даты
       sum(case when cte.diff_m=6 then cte.income_sum_amt else 0 end) SALARY_6M_AMT,
       --  ол-во транзакций за первый мес€ц до отчетной даты
       count(case when cte.diff_m=1 then cte.income_sum_amt else null end) SALARY_1M_CNT,
       --  ол-во транзакций за второй мес€ц до отчетной даты
       count(case when cte.diff_m=2 then cte.income_sum_amt else null end) SALARY_2M_CNT,
       --  ол-во транзакций за третий мес€ц до отчетной даты
       count(case when cte.diff_m=3 then cte.income_sum_amt else null end) SALARY_3M_CNT,
       --  ол-во транзакций за четвертый мес€ц до отчетной даты
       count(case when cte.diff_m=4 then cte.income_sum_amt else null end) SALARY_4M_CNT,
       --  ол-во транзакций за п€тый мес€ц до отчетной даты
       count(case when cte.diff_m=5 then cte.income_sum_amt else null end) SALARY_5M_CNT,
       --  ол-во транзакций за шестой мес€ц до отчетной даты
       count(case when cte.diff_m=6 then cte.income_sum_amt else null end) SALARY_6M_CNT,
       -- ќтношение суммы зарплатных транзакций за последний мес€ц к сумме за 3 последних мес€ца
       round(sum(case when cte.diff_m=1 then cte.income_sum_amt else 0 end)/sum(case when cte.diff_m<=3 then cte.income_sum_amt else 0 end),2) SALARY_1M_TO_3M_AMT_PCT,
       -- ќтношение суммы зарплатных транзакций за последний мес€ц к сумме за 6 последних мес€цев
       round(sum(case when cte.diff_m=1 then cte.income_sum_amt else 0 end)/sum(case when cte.diff_m<=6 then cte.income_sum_amt else 0 end),2) SALARY_1M_TO_6M_AMT_PCT,
       -- ќтношение кол-ва зарплатных транзакций за последний мес€ц к кол-ву за 3 последних мес€ца
       round(count(case when cte.diff_m=1 then cte.income_sum_amt else null end)/count(case when cte.diff_m<=3 then cte.income_sum_amt else null end),2) SALARY_1M_TO_3M_CNT_PCT,
       -- ќтношение кол-ва зарплатных за последний мес€ц к кол-ву за 6 последних мес€цев
       round(count(case when cte.diff_m=1 then cte.income_sum_amt else null end)/count(case when cte.diff_m<=6 then cte.income_sum_amt else null end),2) SALARY_1M_TO_6M_CNT_PCT,
       --  ол-во мес€цев, в которые были зарплатные начислени€, из последних 6 мес€цев
       count(distinct case when cte.diff_m<=6 then cte.diff_m else null end) SALARY_DURING_6M_CNT,
       --  ол-во мес€цев назад, когда была начислена последн€€ зарплатна€ транзакци€
       min(diff_m) LAST_SAL_TRANS_MONTH_CNT,
       --  ол-во мес€цев назад, когда была начислена перва€ зарплатна€ транзакци€
       max(diff_m) FIRST_SAL_TRANS_MONTH_CNT
from cte_fct_income_transactions cte
group by cte.client_id
order by cte.client_id;
