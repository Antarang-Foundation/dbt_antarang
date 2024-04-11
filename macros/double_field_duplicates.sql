
{% macro double_field_duplicates(table, field_1, field_2) %}

with 

t1 as (select * from {{ref(table)}}),
t2 as (select {{ field_1 }}, {{ field_2 }} from t1 group by {{ field_1 }}, {{ field_2 }} having count(*)>1),
t3 as (select t1.* from t1 right join t2 on t1.{{ field_1 }} = t2.{{ field_1 }} and t1.{{ field_2 }} = t2.{{ field_2 }} order by {{ field_1 }}, {{ field_2 }})

select * from t3

{% endmacro %}





