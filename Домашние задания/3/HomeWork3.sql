-- Задание 3

-- Тестовые выборки
/*
select count(employee_id) from hr.employees; --107
select * from hr.employees;
select * from DE_COMMON.TFES_DATASOURCE;
*/

--1.+
/*Выведите список сотрудников (фамилия, имя сотрудника) и их начальников (фамилия, имя) (HR.EMPLOYEES);*/

--select es.employee_id,es.first_name,es.last_name,es.manager_id,em.employee_id,em.first_name,em.last_name
select es.last_name s_fam, es.first_name s_im,em.last_name mng_fam,em.first_name mng_im
from hr.employees es left join hr.employees em on es.manager_id=em.employee_id
order by nvl(es.manager_id,0),es.employee_id;

--2.+
/*Создайте запрос, который позволяет найти корректную почту из DE_COMMON.TFES_DATASOURCE (в скрипте в комментариях необходимо
  указать что Вы считаете корректной электронной почтой), если почта некорректная, то укажите 'некорректная почта' [задача с
  использованием регулярных выражений];
 */

/* корректной электронной почтой - запись вида логин-текст@доменNуровня.домен1уровня -> login@domen2.domen1, login@domen3.domen2.domen1
   длинна логина минимум 1 знак
   символ @ - "коммерческое at" не может быть 1 символом строки
   домен может быть только 1 уровня и содержит хоть 1 буквенный символ
   домены отделяются между собой точками
 */

select case when regexp_instr(td.email,'^\w+@\w+[.]')=0 then 'некорректная почта' else
       regexp_substr(td.email,'^\w+@\w+[.]\w\S+') end good_email,/*Вывод корректного email*/
       td.email, td. first_name, td.last_name, td.gender
from DE_COMMON.TFES_DATASOURCE td;

