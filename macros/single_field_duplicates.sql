{% macro single_field_duplicates(table, field) %}

with 

t1 as (select * from {{ref(table)}}),
t2 as (select {{field}} from t1 group by {{field}} having count(*)>1),
t3 as (select * from t1 where {{field}} in (select * from t2) order by {{field}} )

select * from t3

{% endmacro %}