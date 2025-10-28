with cte as (
                select  a.dynamodb_keys_id_s,
                        a.dynamodb_new_image_individual_m_date_of_birth_s
                from "dynamo_scv_sls_customers" a
                inner join (
                            select  dynamodb_keys_id_s,
                                    max(dynamodb_new_image_updated_at_n) as max_date_up,
                                    min(dynamodb_new_image_updated_at_n) as min_date_up
                            from "dynamo_scv_sls_customers"
                            group by dynamodb_keys_id_s
                            ) t
                on a.dynamodb_keys_id_s=t.dynamodb_keys_id_s and a.dynamodb_new_image_updated_at_n=t.max_date_up
),
cte2 as (
                select        distinct  dynamodb_keys_id_s,
                                DATE(try_cast(dynamodb_new_image_individual_m_date_of_birth_s as date)) as birthday
                from cte
                                where dynamodb_new_image_individual_m_date_of_birth_s is not null
                                and dynamodb_new_image_individual_m_date_of_birth_s != ' '
)
select distinct dynamodb_keys_id_s as dynamo_user_key,
                case when (date_diff('year', birthday,current_date ) >=16 and date_diff('year', birthday,current_date ) <=23) then 1
                else 0 end
                as is_student
                from
                cte2