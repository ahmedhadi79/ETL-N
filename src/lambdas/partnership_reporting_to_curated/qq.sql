select  COALESCE(a.dynamodb_keys_id_s,a.dynamodb_key_id_s) as user_id,
dynamodb_new_image_individual_m_address_m_status_s,
dynamodb_new_image_individual_m_identity_verification_m_status_s, 
dynamodb_new_image_individual_m_nickname_s as Nickname, 
dynamodb_new_image_individual_m_first_name_s as First_Name, 
dynamodb_new_image_individual_m_last_name_s as Last_Name, 
dynamodb_new_image_email_s, 
dynamodb_new_image_phone_number_s, 
dynamodb_new_image_updated_at_n,
dynamodb_new_image_status_s,
dynamodb_new_image_individual_m_address_m_country_code_s,
dynamodb_new_image_brand_id_s,
case 
when date_diff('year', DATE(try_cast(dynamodb_new_image_individual_m_date_of_birth_s as date)) ,current_date )=16 then '16'
when date_diff('year', DATE(try_cast(dynamodb_new_image_individual_m_date_of_birth_s as date)) ,current_date )>=17 and 
      date_diff('year', DATE(try_cast(dynamodb_new_image_individual_m_date_of_birth_s as date)) ,current_date )<=23 then '17_to_23'
when date_diff('year', DATE(try_cast(dynamodb_new_image_individual_m_date_of_birth_s as date)) ,current_date )>=24 and 
      date_diff('year', DATE(try_cast(dynamodb_new_image_individual_m_date_of_birth_s as date)) ,current_date )<=30 then '24_to_30'
when date_diff('year', DATE(try_cast(dynamodb_new_image_individual_m_date_of_birth_s as date)) ,current_date )>=31 and 
      date_diff('year', DATE(try_cast(dynamodb_new_image_individual_m_date_of_birth_s as date)) ,current_date )<=45 then '31_to_45'
when date_diff('year', DATE(try_cast(dynamodb_new_image_individual_m_date_of_birth_s as date)) ,current_date )>=46 then '46+'
end as age_range,
    
case when dynamodb_new_image_individual_m_gender_s like 'Female%' then 'Female' 
         when dynamodb_new_image_individual_m_gender_s like 'Male%' then 'Male' end as Male_Female,
dynamodb_new_image_individual_m_individual_screening_m_result_m_pep_check_passed_bool,
row_number() over(partition by  COALESCE(a.dynamodb_keys_id_s,a.dynamodb_key_id_s) order by dynamodb_new_image_updated_at_n desc) as rn_last,
row_number() over(partition by  COALESCE(a.dynamodb_keys_id_s,a.dynamodb_key_id_s),dynamodb_new_image_status_s order by dynamodb_new_image_updated_at_n asc) as rn_first,
row_number() over(partition by  COALESCE(a.dynamodb_keys_id_s,a.dynamodb_key_id_s),dynamodb_new_image_individual_m_address_m_status_s order by dynamodb_new_image_updated_at_n asc) as rn_add_last,
row_number() over(partition by  COALESCE(a.dynamodb_keys_id_s,a.dynamodb_key_id_s) order by dynamodb_new_image_updated_at_n asc) as rn
from datalake_curated.dynamo_scv_sls_customers a 
where dynamodb_new_image_individual_m_address_m_country_code_s='ARE'