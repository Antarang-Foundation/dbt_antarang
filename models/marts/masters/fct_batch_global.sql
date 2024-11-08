with 
t1 as (select * from {{ref('int_global_session')}}),

t2 as (
    select 
       school_id, school_partner, batch_no, batch_academic_year,
       sum(distinct school_id) 
       sum(distinct school_partner)
       where 

    from t1
    group by batch_academic_year, school_partner          
)

select * from t2


