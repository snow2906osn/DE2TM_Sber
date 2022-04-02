-- Задание 4

-- Тестовые выборки
/*
select count(client_id), count(distinct client_id), count(distinct client_id)*4 from de_common.HW4_FCT_TRANSACTION; --!!! 1015, 8 и 32 (должно быть)
select * from de_common.HW4_FCT_TRANSACTION ft order by ft.client_id, ft.trans_dt;

*/

--1.+
/*На основе таблицы de_common.HW4_FCT_TRANSACTION (хранятся транзакции, совершенные в 2021 году)
  для каждого клиента в разрезе квартала транзакции построить следующие агрегаты:
 */
with MOD_HW4_FCT_TRANSACTION as (
select ft.client_id,
       floor((extract(month from ft.trans_dt)+2)/3) quarter,
       ft.trans_dt,
       ft.trans_amt
from de_common.HW4_FCT_TRANSACTION ft) -- CTE для уменьшения кол-ва интераций с расчетом номера квартала... 1 раз вычислили и используем повторно
select distinct mft.client_id,
       mft.quarter,
       last_value(mft.trans_amt) over
         (partition by mft.client_id, mft.quarter order by mft.trans_dt range between unbounded preceding and unbounded following) sumq_last, -- Сумма последней транзакции по клиенту за квартал
       count(mft.trans_amt) over
         (partition by mft.client_id, mft.quarter) cntq, -- Кол-во транзакций по клиенту за квартал
       max(mft.trans_amt) over
         (partition by mft.client_id, mft.quarter) sumq_max, -- Сумма максимальной транзакции по клиенту за квартал
       round(avg(mft.trans_amt) over
         (partition by mft.client_id, mft.quarter),3) sumq_avg -- Сумма средней транзакции по клиенту за квартал
from MOD_HW4_FCT_TRANSACTION mft
order by mft.client_id, mft.quarter;

--2.+
/*На основе таблицы de_common.HW4_FCT_TRANSACTION для каждого клиента в разрезе месяца вывести
 */
with MOD_HW4_FCT_TRANSACTION as (
select ft.client_id,
       trunc(ft.trans_dt,'MM') month,
       sum(ft.trans_amt) trans_amt
from de_common.HW4_FCT_TRANSACTION ft
group by ft.client_id,trunc(ft.trans_dt,'MM'))
select mft.client_id,
       mft.month,
       mft.trans_amt summ, -- Сумму всех транзакций в текущем месяце
       /*sum(mft.trans_amt) over
         (partition by mft.client_id,mft.month) summ-noneed, -- Сумму всех транзакций в текущем месяце*/
       nvl(lag(mft.trans_amt) over
         (partition by mft.client_id order by mft.month),0) sumprevm, -- Сумму всех транзакций в предыдущем месяце
       nvl(lead(mft.trans_amt) over
         (partition by mft.client_id order by mft.month),0) sumnextm, -- Сумму всех транзакций в следующем месяце
       sum(mft.trans_amt) over
         (partition by mft.client_id order by mft.month rows between unbounded preceding and current row) sumaddit, -- Аддитивную сумму всех транзакций с начала года
       sum(mft.trans_amt) over
         (partition by mft.client_id order by mft.month rows between 2 preceding and 1 following) sumwind -- Сумму транзакций в скользящем окне [-2 месяца; +1 месяц] относительно отчетного месяца
from MOD_HW4_FCT_TRANSACTION mft
order by mft.client_id, mft.month;

--3.+
/*На основе таблицы de_common.HW4_FCT_TRANSACTION для каждого клиента в каждом месяце вывести предпоследнюю транзакцию.
 */

/*В исходной таблице отсутствует уникальный ключ и имеются одинаковые даты (DD-MM-YYYY HH:MM:SS),
  для более достоверной выборки транзакции использую dens_rank по 2 записи
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
                                                (partition by mft.client_id, mft.month order by mft.trans_dt desc) nrow, -- Сортировка по убыванию, в качестве ключа дата... 2 по счету запись являться должна предпоследней
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
