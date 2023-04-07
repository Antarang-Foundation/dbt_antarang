with source as (

    select * from {{ source('salesforce', 'src_State_Govt_Body_Level__c') }}

),

renamed as (

    select
        lastmodifieddate,
        isdeleted,
        contact_name__c,
        lastvieweddate,
        lastreferenceddate,
        name,
        systemmodstamp,
        ownerid,
        createdbyid,
        createddate,
        id,
        state_code__c,
        lastmodifiedbyid,
        _airbyte_ab_id,
        _airbyte_emitted_at,
        _airbyte_normalized_at,
        _airbyte_src_state_govt_body_level__c_hashid,
        _airbyte_unique_key

    from source

)

select * from renamed
