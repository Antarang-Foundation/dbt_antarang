with
    t0 as (
        select *
        from {{ source("salesforce", "Career_Skill__c") }}
        where isdeleted = false
    ),

    t1 as (
        select
            Id as cs_id,
            Barcode__c as assessment_barcode,
            RecordTypeId as record_type_id,
            createddate as created_on,
            Created_from_Form__c as created_from_form,
            name as cs_no,

            grade__c as assessment_grade,
            cast(Academic_Year__c as string) as assessment_academic_year,
            batch_id__c as assessment_batch_id,

            Q_11_1__c as q11_1,
            Q_11_2__c as q11_2,
            Q_11_3__c as q11_3,
            Q_11_4__c as q11_4,
            Q_11_5__c as q11_5,
            Q_11_6__c as q11_6,
            Q_11_7__c as q11_7,
            Q_11_8__c as q11_8,
            Q_11_9__c as q11_9,
            Q_11_ans__c as q11_marks,

            Q_12_1__c as q12_1,
            Q_12_2__c as q12_2,
            Q_12_3__c as q12_3,
            Q_12_4__c as q12_4,
            Q_12_ans__c as q12_marks,

            Q_13__c as q13,
            Q_13_ans__c as q13_marks,

            Q_14__c as q14,
            Q_14_ans__c as q14_marks,

            Q_15_1__c as q15_1,
            Q_15_2__c as q15_2,
            Q_15_3__c as q15_3,
            Q_15_4__c as q15_4,
            Q_15_5__c as q15_5,
            Q_15_6__c as q15_6,
            Q_15_7__c as q15_7,
            Q_15_8__c as q15_8,
            Q_15_9__c as q15_9,
            Q_15_ans__c as q15_marks,

            Q_16__c as q16,
            Q_16_ans__c as q16_marks,

            (
                Q_11_ans__c
                + Q_12_ans__c
                + Q_13_ans__c
                + Q_14_ans__c
                + Q_15_ans__c
                + Q_16_ans__c
            ) as cs_total_marks,
            Q_11_10__c as q11_10,
            Q_11_1_marks__c as q11_1_marks,
            Q_11_2_marks__c as q11_2_marks,
            Q_11_3_marks__c as q11_3_marks,
            Q_11_4_marks__c as q11_4_marks,
            Q_11_5_marks__c as q11_5_marks,
            Q_11_6_marks__c as q11_6_marks,
            Q_11_7_marks__c as q11_7_marks,
            Q_11_8_marks__c as q11_8_marks,
            Q_11_9_marks__c as q11_9_marks,
            Q_11_10_Marks__c as q11_10_marks,

            Q_15_1_marks__c as q15_1_marks,
            Q_15_2_marks__c as q15_2_marks,
            Q_15_3_marks__c as q15_3_marks,
            Q_15_4_marks__c as q15_4_marks,
            Q_15_5_marks__c as q15_5_marks,
            Q_15_6_marks__c as q15_6_marks,
            Q_15_7_marks__c as q15_7_marks,
            Q_15_8_marks__c as q15_8_marks,
            Q_15_9_marks__c as q15_9_marks,

            error_status__c as error_status,
            data_clean_up__c as data_cleanup,
            Marks_Recalculated__c as marks_recalculated,
            Student_Linked__c as student_linked,

        from t0
    ),

    t2 as (select record_type_id, record_type from {{ ref("seed_recordtype") }}),

    t3 as (
        select
            cs_id,
            assessment_barcode,
            record_type,
            created_on,
            created_from_form,
            cs_no,

            (
                case

                    when
                        cs_no is not null
                        and (
                            q11_1 is not null
                            or q11_2 is not null
                            or q11_3 is not null
                            or q11_4 is not null
                            or q11_5 is not null
                            or q11_6 is not null
                            or q11_7 is not null
                            or q11_8 is not null
                            or q11_9 is not null
                            or q11_10 is not null
                            or q12_1 is not null
                            or q12_2 is not null
                            or q12_3 is not null
                            or q12_4 is not null
                            or q13 is not null
                            or q14 is not null
                            or q15_1 is not null
                            or q15_2 is not null
                            or q15_3 is not null
                            or q15_4 is not null
                            or q15_5 is not null
                            or q15_6 is not null
                            or q15_7 is not null
                            or q15_8 is not null
                            or q15_9 is not null
                            or q16 is not null
                        )
                    then 1

                    when
                        cs_no is not null
                        and (
                            q11_1 is null
                            and q11_2 is null
                            and q11_3 is null
                            and q11_4 is null
                            and q11_5 is null
                            and q11_6 is null
                            and q11_7 is null
                            and q11_8 is null
                            and q11_9 is null
                            and q12_1 is null
                            and q12_2 is null
                            and q12_3 is null
                            and q12_4 is null
                            and q13 is null
                            and q14 is null
                            and q15_1 is null
                            and q15_2 is null
                            and q15_3 is null
                            and q15_4 is null
                            and q15_5 is null
                            and q15_6 is null
                            and q15_7 is null
                            and q15_8 is null
                            and q15_9 is null
                            and q16 is null
                        )
                    then 0
                end
            ) is_non_null,

            t1.* except (
                cs_id,
                assessment_barcode,
                record_type_id,
                created_on,
                created_from_form,
                cs_no
            )

        from t1
        left join t2 using (record_type_id)
        order by assessment_barcode, record_type
    )

select *
from t3
