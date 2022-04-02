---Блок 2
/* Тестовые данне
select count(distinct client_id) from de_common.group_dim_client; -- 136 Клиентов
select count(distinct client_id) from de_common.group_fct_credit_applications where to_date('2021-12-01','YYYY-MM-DD')>=application_date; -- 133 Клиента, 3 отсутсвуют!
select count(*) from de_common.group_fct_credit_applications where to_date('2021-12-01','YYYY-MM-DD')>=application_date; -- 191 строка в периоде на дату отчета
 */

-- 3 Клиента (46812,113122,147246) не имеют заявок в отчетном периоде, в поле LAST_APP_MONTH_CNT указано техническое значение -1

ALTER SESSION SET nls_date_language = 'AMERICAN';
ALTER SESSION SET  NLS_DATE_FORMAT='DD-MM-YYYY';

with  mod_group_dict_credit_product as (select
        floor(months_between(to_date('2021-12-01','YYYY-MM-DD'), ca.application_date)) diff_m,
        ca.application_id,
        ca.client_id,
        ca.application_date,
        ca.application_sum_amt,
        cp.product_nm,
        case when floor(months_between(to_date('2021-12-01','YYYY-MM-DD'), ca.application_date))<=6 and cp.product_nm='Ипотека' then 1 else 0 end i_flg
      from de_common.group_fct_credit_applications ca
           inner join de_common.group_dict_credit_product cp on ca.credit_product_type=cp.poduct_type
           and to_date('2021-12-01','YYYY-MM-DD')>=ca.application_date) /* на 01.12.2021 имеется запись */
--/*Для проверки*/select cte.*,round(cte.application_sum_amt) from mod_group_dict_credit_product cte order by client_id
select dc.client_id,
       count(cte.application_id) APP_HIST_CNT,
       count(case when cte.diff_m<=6 then cte.application_id else null end) APP_6M_CNT,
       count(case when cte.diff_m<=3 then cte.application_id else null end) APP_3M_CNT,
       sum(nvl(cte.application_sum_amt,0)) APP_HIST_AMT,
       sum(case when cte.diff_m<=6 then cte.application_sum_amt else 0 end) APP_6M_AMT,
       sum(case when cte.diff_m<=3 then cte.application_sum_amt else 0 end) APP_3M_AMT,
       max(nvl(cte.i_flg,0)) MORTGAGE_6M_FLG,
       min(nvl(cte.diff_m,-1)) LAST_APP_MONTH_CNT
from de_common.group_dim_client dc
     left join mod_group_dict_credit_product cte on dc.client_id=cte.client_id
group by dc.client_id
order by dc.client_id
