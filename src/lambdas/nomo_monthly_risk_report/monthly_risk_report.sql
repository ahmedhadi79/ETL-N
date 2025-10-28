With customer as (
    SELECT concat(
            dynamodb_new_image_individual_m_first_name_s,
            ' ',
            dynamodb_new_image_individual_m_last_name_s
        ) as name,
        dynamodb_new_image_phone_number_s,
        dynamodb_new_image_status_s,
        COALESCE(
            dynamodb_keys_id_s,
            dynamodb_key_id_s,
            dynamodb_new_image_id_s
        ) as user_id,
        dynamodb_keys_id_s,
        dynamodb_new_image_email_s,
        dynamodb_new_image_individual_m_first_name_s,
        dynamodb_new_image_individual_m_last_name_s,
        dynamodb_new_image_preferences_m_email_notifications_m_marketing_bool,
        dynamodb_new_image_preferences_m_sms_notifications_m_marketing_bool,
        dynamodb_new_image_type_s,
        dynamodb_new_image_individual_m_nationality_country_code_s as nationality_country_code_s,
        dynamodb_new_image_individual_m_address_m_country_code_s as address_country_code_s,
        ROW_NUMBER () OVER (
            PARTITION BY COALESCE(
                dynamodb_keys_id_s,
                dynamodb_key_id_s,
                dynamodb_new_image_id_s
            )
            ORDER BY dynamodb_new_image_updated_at_n DESC
        ) customer_rn
    FROM datalake_raw.dynamo_scv_sls_customers
),
customer_rn as (
    Select * ,
        Row_number() over(
            order by user_id
        ) as customer_id_generated
    From customer
    Where customer_rn = 1
),
clients as (
    Select *,
        ROW_NUMBER() OVER (
            PARTITION BY "id",
            encoded_key
            ORDER BY "last_modified_date" DESC
        ) AS rn
    From datalake_raw.clients
),
clients_rn as (
    Select *
    From clients
    Where rn = 1
),
deposit_accounts as (
    Select id,
        account_holder_key,
        product_type_key,
        account_type,
        account_state,
        custom_fields_0_value,
        custom_fields_1_value,
        custom_fields_2_value,
        encoded_key,
        cast(balances_total_balance as decimal(20, 2)) as balances_total_balance,
        name,
        currency_code,
        CASE
            WHEN currency_code = 'GBP' THEN cast(balances_total_balance as decimal(20, 2))
            WHEN currency_code = 'USD' THEN cast(balances_total_balance as decimal(20, 2)) * 0.78
        END AS gbp_balances_total_balance,
        approved_date,
        ROW_NUMBER() OVER (
            PARTITION BY "id"
            ORDER BY "last_modified_date" DESC
        ) AS rn,
        accrued_amounts_interest_accrued,
        interest_settings_interest_rate_settings_interest_rate,
        maturity_date
    From datalake_raw.deposit_accounts
),
deposit_accounts_rn as (
    Select *
    From deposit_accounts
    Where rn = 1
)
Select customer_rn.customer_id_generated,
    -- ,customer_rn.dynamodb_keys_id_s
    customer_rn.nationality_country_code_s,
    customer_rn.address_country_code_s,
    deposit_accounts_rn.currency_code,
    deposit_accounts_rn.balances_total_balance,
    deposit_accounts_rn.accrued_amounts_interest_accrued as accrued_amounts_profit_accrued,
    deposit_accounts_rn.name as account_Type,
    CAST(
        From_iso8601_timestamp(deposit_accounts_rn.approved_date) as date
    ) as Account_open_date,
    CAST(
        From_iso8601_timestamp(deposit_accounts_rn.maturity_date) as date
    ) as maturity_date,
    deposit_accounts_rn.interest_settings_interest_rate_settings_interest_rate as profit_rate
From customer_rn
    LEFT OUTER JOIN clients_rn ON clients_rn.id = customer_rn.dynamodb_keys_id_s
    LEFT OUTER JOIN deposit_accounts_rn ON deposit_accounts_rn.account_holder_key = clients_rn.encoded_key
Where 1 = 1
    AND customer_rn.dynamodb_new_image_status_s = 'APPROVED'