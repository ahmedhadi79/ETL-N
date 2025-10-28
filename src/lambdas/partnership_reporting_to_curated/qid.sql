with info as (
select dynamodb_new_image_customer_id_s, 
dynamodb_new_image_updated_at_n, 
row_number() over(partition by dynamodb_new_image_customer_id_s order by dynamodb_new_image_updated_at_n desc ) as rn,
concat(dynamodb_new_image_form_data_m_customer_circumstances_l_0_s,'| ',
dynamodb_new_image_form_data_m_customer_circumstances_l_1_s, '| ',
dynamodb_new_image_form_data_m_customer_circumstances_l_2_s,'| ',
dynamodb_new_image_form_data_m_customer_circumstances_l_3_s,'| ',
dynamodb_new_image_form_data_m_customer_circumstances_l_4_s,'| ',
dynamodb_new_image_form_data_m_customer_circumstances_l_5_s) as Circumstance
 from datalake_raw.dynamo_sls_customer_risk_form
 )
 select *,
 case when Circumstance like '%SELF_EMPLOYED%' or Circumstance like '%BUSINESS_OWNER%' then 'Y' 
      when Circumstance is null or ltrim(rtrim(Circumstance))='' then null
      else 'N'
 end as SelfEmployed_OR_BusinessOwner
 from info