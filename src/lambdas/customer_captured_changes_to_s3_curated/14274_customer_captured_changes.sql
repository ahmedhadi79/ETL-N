with all_key as (
    select case
            when dynamodb_keys_id_s is null then dynamodb_key_id_s
            else dynamodb_keys_id_s
        end as user_id,
        *
    from datalake_curated.dynamo_scv_sls_customers
),
rws as (
    select *,
        row_number() over(
            partition by user_id
            order by dynamodb_new_image_updated_at_n desc
        ) as rn_last,
        row_number() over(
            partition by user_id,
            dynamodb_new_image_status_s
            order by dynamodb_new_image_updated_at_n asc
        ) as rn_first,
        row_number() over(
            partition by user_id
            order by dynamodb_new_image_updated_at_n asc
        ) as rn
    from all_key
    where dynamodb_new_image_updated_at_n is not null
),
first_ as (
    select distinct user_id,
        dynamodb_new_image_updated_at_n as first_join_date
    from rws
    where rn = 1
),
final_ as (
    select user_id as dynamo_user_key,
        dynamodb_new_image_updated_at_n as dynamo_updated_at,
        dynamodb_new_image_status_s as dynamo_user_status,
        dynamodb_new_image_individual_m_nationality_country_code_s as dynamodb_individual_m_nationality_country_code_s,
        case
            when (
                dynamodb_new_image_phone_number_s is not null
                and ltrim(rtrim(dynamodb_new_image_phone_number_s)) != ''
            ) then 1
            else 0
        end as phone_number_populated,
        case
            when (
                dynamodb_new_image_email_s is not null
                and ltrim(rtrim(dynamodb_new_image_email_s)) != ''
            ) then 1
            else 0
        end as email_populated,
        case
            when (
                dynamodb_new_image_individual_m_nickname_s is not null
                and ltrim(
                    rtrim(dynamodb_new_image_individual_m_nickname_s)
                ) != ''
            ) then 1
            else 0
        end as nickname_populated,
        dynamodb_new_image_individual_m_address_m_country_code_s as address_country_code,
        dynamodb_new_image_individual_m_address_m_status_s as address_status,
        dynamodb_new_image_individual_m_financial_details_m_asset_types_l_0_s as asset_types,
        dynamodb_new_image_individual_m_financial_details_m_estimated_assets_value_s as estimated_assets_value,
        dynamodb_new_image_individual_m_financial_details_m_income_sources_l_0_s as income_sources,
        dynamodb_new_image_individual_m_financial_details_m_monthly_income_s as monthly_income,
        dynamodb_new_image_individual_m_financial_details_m_reason_for_opening_account_l_0_s as reason_for_opening_account,
        dynamodb_new_image_individual_m_identity_verification_m_status_s as identity_verification_status,
        dynamodb_new_image_individual_m_tax_ids_l_0_m_country_code_s as tax_ids_country_code,
        dynamodb_new_image_brand_id_s as brand_id,
        case
            when (
                dynamodb_new_image_individual_m_tax_ids_l_0_m_value_s is not null
                and ltrim(
                    rtrim(
                        dynamodb_new_image_individual_m_tax_ids_l_0_m_value_s
                    )
                ) != ''
            ) then 1
            else 0
        end as tax_id_value_populated,
        case
            when (
                dynamodb_new_image_individual_m_tax_ids_l_0_m_skip_reason_s is not null
                and ltrim(
                    rtrim(
                        dynamodb_new_image_individual_m_tax_ids_l_0_m_skip_reason_s
                    )
                ) != ''
            ) then 1
            else 0
        end as tax_id_skip_reason_populated,
        dynamodb_new_image_external_onfido_applicant_id_s as external_onfido_applicant_id,
        dynamodb_new_image_individual_m_identity_verification_m_document_type_s as identity_document_type,
        case
            when date_diff(
                'year',
                DATE(
                    try_cast(
                        dynamodb_new_image_individual_m_date_of_birth_s as date
                    )
                ),
                current_date
            ) >= 17
            and date_diff(
                'year',
                DATE(
                    try_cast(
                        dynamodb_new_image_individual_m_date_of_birth_s as date
                    )
                ),
                current_date
            ) <= 23 then '17_to_23'
            when date_diff(
                'year',
                DATE(
                    try_cast(
                        dynamodb_new_image_individual_m_date_of_birth_s as date
                    )
                ),
                current_date
            ) >= 24
            and date_diff(
                'year',
                DATE(
                    try_cast(
                        dynamodb_new_image_individual_m_date_of_birth_s as date
                    )
                ),
                current_date
            ) <= 30 then '24_to_30'
            when date_diff(
                'year',
                DATE(
                    try_cast(
                        dynamodb_new_image_individual_m_date_of_birth_s as date
                    )
                ),
                current_date
            ) >= 31
            and date_diff(
                'year',
                DATE(
                    try_cast(
                        dynamodb_new_image_individual_m_date_of_birth_s as date
                    )
                ),
                current_date
            ) <= 45 then '31_to_45'
            when date_diff(
                'year',
                DATE(
                    try_cast(
                        dynamodb_new_image_individual_m_date_of_birth_s as date
                    )
                ),
                current_date
            ) >= 46 then '46+'
        end as age_range,
        date_diff(
            'year',
            DATE(
                try_cast(
                    dynamodb_new_image_individual_m_date_of_birth_s as date
                )
            ),
            current_date
        ) as age,
        case
            when dynamodb_new_image_individual_m_gender_s like 'Female%' then 1
            when dynamodb_new_image_individual_m_gender_s like 'Male%' then 0
        end as dynamodb_new_gid,
        dynamodb_new_image_preferences_m_push_notifications_m_marketing_bool as push_notifications_m_marketing,
        dynamodb_new_image_preferences_m_sms_notifications_m_marketing_bool as sms_notifications_m_marketing,
        dynamodb_new_image_preferences_m_email_notifications_m_marketing_bool as email_notifications_m_marketing
    from rws
    where rn_last = 1
)
select a.*,
    b.first_join_date
from final_ a
    left join first_ b on a.dynamo_user_key = b.user_id