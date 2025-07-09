SELECT distinct school_name, school_district,
   school_area,school_taluka,school_partner,batch_academic_year,
        CASE WHEN COUNTIF(session_date is null or total_student_present is null)>0 
        THEN 'Not Complete'
            ELSE 'Completed'
        END AS is_batch_completed
from {{ ref('fct_global_session') }}
    where batch_academic_year >= 2023
    GROUP BY batch_academic_year,school_name,school_district,
school_area,school_taluka,school_partner