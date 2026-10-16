with
    source as (

        select
            school_state,
            school_district,
            school_taluka,
            school_ward,
            school_partner,
            batch_academic_year,
            first_barcode,
            student_barcode,
            g9_barcode,
            g10_barcode,
            g11_barcode,
            g12_barcode,
            student_name,
            gender,
            birth_year,
            school_name,
            batch_no,
            batch_grade,
            facilitator_name,
            facilitator_email,
            g9_whatsapp_no,
            g10_whatsapp_no,
            g11_whatsapp_no,
            g12_whatsapp_no
        from {{ ref("dev_int_global_dcp") }}

    ),

    latest_year as (

        select max(safe_cast(batch_academic_year as int64)) as academic_year from source

    ),

    mapping as (

        select
            s.*,

            case
                when batch_grade = 'Grade 9'
                then 'Grade 10'
                when batch_grade = 'Grade 10'
                then 'Grade 11'
                when batch_grade = 'Grade 11'
                then 'Grade 12'
                when batch_grade = 'Grade 12'
                then 'Passed Grade 12'
            end as current_grade,

            case
                when
                    first_barcode != student_barcode
                    or (g9_barcode is not null and g9_barcode != student_barcode)
                    or (g10_barcode is not null and g10_barcode != student_barcode)
                    or (g11_barcode is not null and g11_barcode != student_barcode)
                    or (g12_barcode is not null and g12_barcode != student_barcode)
                then 'Linked'
                else 'Not Linked'
            end as linking_status

        from source s

    ),

    student_barcodes as (

        select
            m.*,
            coalesce(
                cast(first_barcode as string), cast(student_barcode as string)
            ) as student_key
        from mapping m

    ),

    pre_batch_candidates as (

        select
            curr.student_key,
            previous.batch_no as pre_batch_no,
            safe_cast(previous.batch_academic_year as int64) as pre_batch_academic_year

        from student_barcodes curr

        join
            student_barcodes previous
            on curr.student_key = previous.student_key
            and safe_cast(previous.batch_academic_year as int64)
            < safe_cast(curr.batch_academic_year as int64)
            and previous.batch_no is not null

        qualify
            row_number() over (
                partition by curr.student_key, curr.batch_academic_year
                order by safe_cast(previous.batch_academic_year as int64) desc
            )
            = 1

    ),

    latest_student_batch as (

        select
            s.student_key,
            s.batch_no as current_batch_no,
            s.linking_status as current_linking_status

        from student_barcodes s

        cross join latest_year y

        where safe_cast(s.batch_academic_year as int64) = y.academic_year

        qualify
            row_number() over (partition by s.student_key order by s.batch_no desc) = 1

    ),

    latest_student_whatsapp as (

        select
            student_key,
            coalesce(
                g12_whatsapp_no, g11_whatsapp_no, g10_whatsapp_no, g9_whatsapp_no
            ) as latest_whatsapp_no

        from student_barcodes

        qualify
            row_number() over (
                partition by student_key
                order by safe_cast(batch_academic_year as int64) desc
            )
            = 1

    ),

    final as (

        select
            s.school_state,
            s.school_district,
            s.school_taluka,
            s.school_ward,
            s.school_partner,
            s.batch_academic_year,
            s.first_barcode,
            s.student_barcode,
            s.g9_barcode,
            s.g10_barcode,
            s.g11_barcode,
            s.g12_barcode,
            s.student_name,
            s.gender,
            s.birth_year,
            s.school_name,
            s.batch_no,
            s.batch_grade,
            s.facilitator_name,
            s.facilitator_email,
            latest_whatsapp_no,
            s.current_grade,
            s.linking_status,
            coalesce(p.pre_batch_no, s.batch_no) as pre_batch_no,

            case
                when l.current_linking_status = 'Linked' then 'Linked' else 'Unlinked'
            end as uid_status,

            case
                when l.current_linking_status = 'Linked'
                then l.current_batch_no
                else null
            end as current_batch_no,

            s.facilitator_name as latest_facilitator_name,
            s.facilitator_email as latest_facilitator_email

        from student_barcodes s

        left join
            pre_batch_candidates p
            on s.student_key = p.student_key
            and safe_cast(s.batch_academic_year as int64)
            = p.pre_batch_academic_year + 1

        left join latest_student_batch l on s.student_key = l.student_key

        left join latest_student_whatsapp w on s.student_key = w.student_key
    )

select *
from final
