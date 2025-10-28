with cte as (
select distinct
dynamodb_keys_customer_id_s,
case 
    when ( items_new_0_type='SAVINGS_FROM_SALARY_OR_EARNINGS' or items_new_0_type='INCOME_FROM_SALARY_BONUS_BENEFITS_COMMISSIONS'
or items_new_0_type='INCOME_FROM_PERSONAL_SAVINGS') then 'income_salary' else 'wealth' end as "items_new_0_type",
COALESCE( items_new_0_converted_currency, '0_0_0' ) as "items_new_0_converted_currency"	,

case 
    when ( items_new_1_type='SAVINGS_FROM_SALARY_OR_EARNINGS' or items_new_1_type='INCOME_FROM_SALARY_BONUS_BENEFITS_COMMISSIONS'
or items_new_1_type='INCOME_FROM_PERSONAL_SAVINGS') then 'income_salary' else 'wealth' end as "items_new_1_type",     
COALESCE( items_new_1_converted_currency , '0_0_0' ) as "items_new_1_converted_currency",

case 
    when ( items_new_2_type='SAVINGS_FROM_SALARY_OR_EARNINGS' or items_new_2_type='INCOME_FROM_SALARY_BONUS_BENEFITS_COMMISSIONS'
or items_new_2_type='INCOME_FROM_PERSONAL_SAVINGS') then 'income_salary' else 'wealth' end as "items_new_2_type",
COALESCE( items_new_2_converted_currency, '0_0_0' ) as "items_new_2_converted_currency"	,

case when ( items_new_3_type='SAVINGS_FROM_SALARY_OR_EARNINGS' or items_new_3_type='INCOME_FROM_SALARY_BONUS_BENEFITS_COMMISSIONS'
or items_new_3_type='INCOME_FROM_PERSONAL_SAVINGS') then 'income_salary' else 'wealth' end as "items_new_3_type",	  	              	
COALESCE( items_new_3_converted_currency, '0_0_0' ) as "items_new_3_converted_currency"	,

case when ( items_new_4_type='SAVINGS_FROM_SALARY_OR_EARNINGS' or items_new_4_type='INCOME_FROM_SALARY_BONUS_BENEFITS_COMMISSIONS'
or items_new_4_type='INCOME_FROM_PERSONAL_SAVINGS') then 'income_salary' else 'wealth' end as "items_new_4_type",
COALESCE( items_new_4_converted_currency, '0_0_0' ) as "items_new_4_converted_currency"	,

case when ( items_new_5_type='SAVINGS_FROM_SALARY_OR_EARNINGS' or items_new_5_type='INCOME_FROM_SALARY_BONUS_BENEFITS_COMMISSIONS'
or items_new_5_type='INCOME_FROM_PERSONAL_SAVINGS') then 'income_salary' else 'wealth' end as "items_new_5_type",
COALESCE( items_new_5_converted_currency, '0_0_0' ) as "items_new_5_converted_currency"	,

case when ( items_new_6_type='SAVINGS_FROM_SALARY_OR_EARNINGS' or items_new_6_type='INCOME_FROM_SALARY_BONUS_BENEFITS_COMMISSIONS'
or items_new_6_type='INCOME_FROM_PERSONAL_SAVINGS') then 'income_salary' else 'wealth' end as "items_new_6_type",
COALESCE( items_new_6_converted_currency, '0_0_0' ) as "items_new_6_converted_currency"	,

case when ( items_new_7_type='SAVINGS_FROM_SALARY_OR_EARNINGS' or items_new_7_type='INCOME_FROM_SALARY_BONUS_BENEFITS_COMMISSIONS'
or items_new_7_type='INCOME_FROM_PERSONAL_SAVINGS') then 'income_salary' else 'wealth' end as "items_new_7_type",
COALESCE( items_new_7_converted_currency, '0_0_0' ) as "items_new_7_converted_currency"	,

case when ( items_new_8_type='SAVINGS_FROM_SALARY_OR_EARNINGS' or items_new_8_type='INCOME_FROM_SALARY_BONUS_BENEFITS_COMMISSIONS'
or items_new_8_type='INCOME_FROM_PERSONAL_SAVINGS') then 'income_salary' else 'wealth' end as "items_new_8_type",
COALESCE( items_new_8_converted_currency, '0_0_0' ) as "items_new_8_converted_currency"	,

case when ( items_new_9_type='SAVINGS_FROM_SALARY_OR_EARNINGS' or items_new_9_type='INCOME_FROM_SALARY_BONUS_BENEFITS_COMMISSIONS'
or items_new_9_type='INCOME_FROM_PERSONAL_SAVINGS') then 'income_salary' else 'wealth' end as "items_new_9_type",    
COALESCE( items_new_9_converted_currency, '0_0_0' ) as "items_new_9_converted_currency"	,

case when ( items_new_10_type='SAVINGS_FROM_SALARY_OR_EARNINGS' or items_new_10_type='INCOME_FROM_SALARY_BONUS_BENEFITS_COMMISSIONS'
or items_new_10_type='INCOME_FROM_PERSONAL_SAVINGS') then 'income_salary' else 'wealth' end as "items_new_10_type", 
COALESCE( items_new_10_converted_currency, '0_0_0' ) as "items_new_10_converted_currency"	,

case when ( items_new_11_type='SAVINGS_FROM_SALARY_OR_EARNINGS' or items_new_11_type='INCOME_FROM_SALARY_BONUS_BENEFITS_COMMISSIONS'
or items_new_11_type='INCOME_FROM_PERSONAL_SAVINGS') then 'income_salary' else 'wealth' end as "items_new_11_type",
COALESCE( items_new_11_converted_currency, '0_0_0' ) as "items_new_11_converted_currency"	, 

case when ( items_new_12_type='SAVINGS_FROM_SALARY_OR_EARNINGS' or items_new_12_type='INCOME_FROM_SALARY_BONUS_BENEFITS_COMMISSIONS'
or items_new_12_type='INCOME_FROM_PERSONAL_SAVINGS') then 'income_salary' else 'wealth' end as "items_new_12_type",
COALESCE( items_new_12_converted_currency, '0_0_0' ) as "items_new_12_converted_currency"	,

case when ( items_new_13_type='SAVINGS_FROM_SALARY_OR_EARNINGS' or items_new_13_type='INCOME_FROM_SALARY_BONUS_BENEFITS_COMMISSIONS'
or items_new_13_type='INCOME_FROM_PERSONAL_SAVINGS') then 'income_salary' else 'wealth' end as "items_new_13_type",
COALESCE( items_new_13_converted_currency	, '0_0_0' ) as "items_new_13_converted_currency"	,

case when ( items_new_14_type='SAVINGS_FROM_SALARY_OR_EARNINGS' or items_new_14_type='INCOME_FROM_SALARY_BONUS_BENEFITS_COMMISSIONS'
or items_new_14_type='INCOME_FROM_PERSONAL_SAVINGS') then 'income_salary' else 'wealth' end as "items_new_14_type",
COALESCE( items_new_14_converted_currency, '0_0_0' ) as "items_new_14_converted_currency"	,

case when ( items_new_15_type='SAVINGS_FROM_SALARY_OR_EARNINGS' or items_new_15_type='INCOME_FROM_SALARY_BONUS_BENEFITS_COMMISSIONS'
or items_new_15_type='INCOME_FROM_PERSONAL_SAVINGS') then 'income_salary' else 'wealth' end as "items_new_15_type",
COALESCE( items_new_15_converted_currency, '0_0_0' ) as "items_new_15_converted_currency"	,

case when ( items_new_16_type='SAVINGS_FROM_SALARY_OR_EARNINGS' or items_new_16_type='INCOME_FROM_SALARY_BONUS_BENEFITS_COMMISSIONS'
or items_new_16_type='INCOME_FROM_PERSONAL_SAVINGS') then 'income_salary' else 'wealth' end as "items_new_16_type",
COALESCE( items_new_16_converted_currency, '0_0_0' ) as "items_new_16_converted_currency"	,

case when ( items_new_17_type='SAVINGS_FROM_SALARY_OR_EARNINGS' or items_new_17_type='INCOME_FROM_SALARY_BONUS_BENEFITS_COMMISSIONS'
or items_new_17_type='INCOME_FROM_PERSONAL_SAVINGS') then 'income_salary' else 'wealth' end as "items_new_17_type",
COALESCE( items_new_17_converted_currency, '0_0_0' ) as "items_new_17_converted_currency"	,

case when ( items_new_18_type='SAVINGS_FROM_SALARY_OR_EARNINGS' or items_new_18_type='INCOME_FROM_SALARY_BONUS_BENEFITS_COMMISSIONS'
or items_new_18_type='INCOME_FROM_PERSONAL_SAVINGS') then 'income_salary' else 'wealth' end as "items_new_18_type",
COALESCE( items_new_18_converted_currency, '0_0_0' ) as "items_new_18_converted_currency"	,

case when ( items_new_19_type='SAVINGS_FROM_SALARY_OR_EARNINGS' or items_new_19_type='INCOME_FROM_SALARY_BONUS_BENEFITS_COMMISSIONS'
or items_new_19_type='INCOME_FROM_PERSONAL_SAVINGS') then 'income_salary' else 'wealth' end as "items_new_19_type",
COALESCE( items_new_19_converted_currency, '0_0_0' ) as "items_new_19_converted_currency"	,

case when ( items_new_20_type='SAVINGS_FROM_SALARY_OR_EARNINGS' or items_new_20_type='INCOME_FROM_SALARY_BONUS_BENEFITS_COMMISSIONS'
or items_new_20_type='INCOME_FROM_PERSONAL_SAVINGS') then 'income_salary' else 'wealth' end as "items_new_20_type",
COALESCE( items_new_20_converted_currency	, '0_0_0' ) as "items_new_20_converted_currency"	,

case when ( items_new_21_type='SAVINGS_FROM_SALARY_OR_EARNINGS' or items_new_21_type='INCOME_FROM_SALARY_BONUS_BENEFITS_COMMISSIONS'
or items_new_21_type='INCOME_FROM_PERSONAL_SAVINGS') then 'income_salary' else 'wealth' end as "items_new_21_type",
COALESCE( items_new_21_converted_currency, '0_0_0' ) as "items_new_21_converted_currency"	,

case when ( items_new_22_type='SAVINGS_FROM_SALARY_OR_EARNINGS' or items_new_22_type='INCOME_FROM_SALARY_BONUS_BENEFITS_COMMISSIONS'
or items_new_22_type='INCOME_FROM_PERSONAL_SAVINGS') then 'income_salary' else 'wealth' end as "items_new_22_type",
COALESCE( items_new_22_converted_currency, '0_0_0' ) as "items_new_22_converted_currency"	,

case when ( items_new_23_type='SAVINGS_FROM_SALARY_OR_EARNINGS' or items_new_23_type='INCOME_FROM_SALARY_BONUS_BENEFITS_COMMISSIONS'
or items_new_23_type='INCOME_FROM_PERSONAL_SAVINGS') then 'income_salary' else 'wealth' end as "items_new_23_type",
COALESCE( items_new_23_converted_currency, '0_0_0' ) as "items_new_23_converted_currency"	,

case when ( items_new_24_type='SAVINGS_FROM_SALARY_OR_EARNINGS' or items_new_24_type='INCOME_FROM_SALARY_BONUS_BENEFITS_COMMISSIONS'
or items_new_24_type='INCOME_FROM_PERSONAL_SAVINGS') then 'income_salary' else 'wealth' end as "items_new_24_type",
COALESCE( items_new_24_converted_currency, '0_0_0' ) as "items_new_24_converted_currency"	,

case when ( items_new_25_type='SAVINGS_FROM_SALARY_OR_EARNINGS' or items_new_25_type='INCOME_FROM_SALARY_BONUS_BENEFITS_COMMISSIONS'
or items_new_25_type='INCOME_FROM_PERSONAL_SAVINGS') then 'income_salary' else 'wealth' end as "items_new_25_type",
COALESCE( items_new_25_converted_currency, '0_0_0' ) as "items_new_25_converted_currency"	,

case when ( items_new_26_type='SAVINGS_FROM_SALARY_OR_EARNINGS' or items_new_26_type='INCOME_FROM_SALARY_BONUS_BENEFITS_COMMISSIONS'
or items_new_26_type='INCOME_FROM_PERSONAL_SAVINGS') then 'income_salary' else 'wealth' end as "items_new_26_type",
COALESCE( items_new_26_converted_currency, '0_0_0' ) as "items_new_26_converted_currency"	,

case when ( items_new_27_type='SAVINGS_FROM_SALARY_OR_EARNINGS' or items_new_27_type='INCOME_FROM_SALARY_BONUS_BENEFITS_COMMISSIONS'
or items_new_27_type='INCOME_FROM_PERSONAL_SAVINGS') then 'income_salary' else 'wealth' end as "items_new_27_type",
COALESCE( items_new_27_converted_currency	, '0_0_0' ) as "items_new_27_converted_currency"	,

case when ( items_new_28_type='SAVINGS_FROM_SALARY_OR_EARNINGS' or items_new_28_type='INCOME_FROM_SALARY_BONUS_BENEFITS_COMMISSIONS'
or items_new_28_type='INCOME_FROM_PERSONAL_SAVINGS') then 'income_salary' else 'wealth' end as "items_new_28_type",
COALESCE( items_new_28_converted_currency, '0_0_0' ) as "items_new_28_converted_currency"	,

case when ( items_new_29_type='SAVINGS_FROM_SALARY_OR_EARNINGS' or items_new_29_type='INCOME_FROM_SALARY_BONUS_BENEFITS_COMMISSIONS'
or items_new_29_type='INCOME_FROM_PERSONAL_SAVINGS') then 'income_salary' else 'wealth' end as "items_new_29_type",
COALESCE( items_new_29_converted_currency, '0_0_0' ) as "items_new_29_converted_currency"	
from "datalake_curated"."dynamo_sls_riskscore"
) 
select 
dynamodb_keys_customer_id_s as "customer_id",
sum (
case when items_new_0_type = 'income_salary' then try_cast(COALESCE( ELEMENT_AT(SPLIT(items_new_0_converted_currency, '_') ,2),'0') as int) else 0 end + 
case when items_new_1_type = 'income_salary' then try_cast(COALESCE( ELEMENT_AT(SPLIT(items_new_1_converted_currency, '_') ,2),'0') as int) else 0 end + 
case when items_new_2_type = 'income_salary' then try_cast(COALESCE( ELEMENT_AT(SPLIT(items_new_0_converted_currency, '_') ,2),'0') as int) else 0 end + 
case when items_new_3_type = 'income_salary' then try_cast(COALESCE( ELEMENT_AT(SPLIT(items_new_1_converted_currency, '_') ,2),'0') as int) else 0 end + 
case when items_new_4_type = 'income_salary' then try_cast(COALESCE( ELEMENT_AT(SPLIT(items_new_0_converted_currency, '_') ,2),'0') as int) else 0 end + 
case when items_new_5_type = 'income_salary' then try_cast(COALESCE( ELEMENT_AT(SPLIT(items_new_5_converted_currency, '_') ,2),'0') as int) else 0 end + 
case when items_new_6_type = 'income_salary' then try_cast(COALESCE( ELEMENT_AT(SPLIT(items_new_6_converted_currency, '_') ,2),'0') as int) else 0 end + 
case when items_new_7_type = 'income_salary' then try_cast(COALESCE( ELEMENT_AT(SPLIT(items_new_7_converted_currency, '_') ,2),'0') as int) else 0 end + 
case when items_new_8_type = 'income_salary' then try_cast(COALESCE( ELEMENT_AT(SPLIT(items_new_8_converted_currency, '_') ,2),'0') as int) else 0 end + 
case when items_new_9_type = 'income_salary' then try_cast(COALESCE( ELEMENT_AT(SPLIT(items_new_9_converted_currency, '_') ,2),'0') as int) else 0 end + 
case when items_new_10_type = 'income_salary' then try_cast(COALESCE( ELEMENT_AT(SPLIT(items_new_10_converted_currency, '_') ,2),'0') as int) else 0 end + 
case when items_new_11_type = 'income_salary' then try_cast(COALESCE( ELEMENT_AT(SPLIT(items_new_11_converted_currency, '_') ,2),'0') as int) else 0 end + 
case when items_new_12_type = 'income_salary' then try_cast(COALESCE( ELEMENT_AT(SPLIT(items_new_12_converted_currency, '_') ,2),'0') as int) else 0 end + 
case when items_new_13_type = 'income_salary' then try_cast(COALESCE( ELEMENT_AT(SPLIT(items_new_13_converted_currency, '_') ,2),'0') as int) else 0 end + 
case when items_new_14_type = 'income_salary' then try_cast(COALESCE( ELEMENT_AT(SPLIT(items_new_14_converted_currency, '_') ,2),'0') as int) else 0 end + 
case when items_new_15_type = 'income_salary' then try_cast(COALESCE( ELEMENT_AT(SPLIT(items_new_15_converted_currency, '_') ,2),'0') as int) else 0 end +
case when items_new_16_type = 'income_salary' then try_cast(COALESCE( ELEMENT_AT(SPLIT(items_new_16_converted_currency, '_') ,2),'0') as int) else 0 end + 
case when items_new_17_type = 'income_salary' then try_cast(COALESCE( ELEMENT_AT(SPLIT(items_new_17_converted_currency, '_') ,2),'0') as int) else 0 end + 
case when items_new_18_type = 'income_salary' then try_cast(COALESCE( ELEMENT_AT(SPLIT(items_new_18_converted_currency, '_') ,2),'0') as int) else 0 end + 
case when items_new_19_type = 'income_salary' then try_cast(COALESCE( ELEMENT_AT(SPLIT(items_new_19_converted_currency, '_') ,2),'0') as int) else 0 end + 
case when items_new_20_type = 'income_salary' then try_cast(COALESCE( ELEMENT_AT(SPLIT(items_new_20_converted_currency, '_') ,2),'0') as int) else 0 end + 
case when items_new_21_type = 'income_salary' then try_cast(COALESCE( ELEMENT_AT(SPLIT(items_new_21_converted_currency, '_') ,2),'0') as int) else 0 end + 
case when items_new_22_type = 'income_salary' then try_cast(COALESCE( ELEMENT_AT(SPLIT(items_new_22_converted_currency, '_') ,2),'0') as int) else 0 end + 
case when items_new_23_type = 'income_salary' then try_cast(COALESCE( ELEMENT_AT(SPLIT(items_new_23_converted_currency, '_') ,2),'0') as int) else 0 end + 
case when items_new_24_type = 'income_salary' then try_cast(COALESCE( ELEMENT_AT(SPLIT(items_new_24_converted_currency, '_') ,2),'0') as int) else 0 end + 
case when items_new_25_type = 'income_salary' then try_cast(COALESCE( ELEMENT_AT(SPLIT(items_new_25_converted_currency, '_') ,2),'0') as int) else 0 end + 
case when items_new_26_type = 'income_salary' then try_cast(COALESCE( ELEMENT_AT(SPLIT(items_new_26_converted_currency, '_') ,2),'0') as int) else 0 end + 
case when items_new_27_type = 'income_salary' then try_cast(COALESCE( ELEMENT_AT(SPLIT(items_new_27_converted_currency, '_') ,2),'0') as int) else 0 end + 
case when items_new_28_type = 'income_salary' then try_cast(COALESCE( ELEMENT_AT(SPLIT(items_new_28_converted_currency, '_') ,2),'0') as int) else 0 end + 
case when items_new_29_type = 'income_salary' then try_cast(COALESCE( ELEMENT_AT(SPLIT(items_new_29_converted_currency, '_') ,2),'0') as int) else 0 end 
)as "Income_less_GBP",
sum (
case when items_new_0_type = 'wealth' then try_cast(COALESCE( ELEMENT_AT(SPLIT(items_new_0_converted_currency, '_') ,2),'0') as int) else 0 end + 
case when items_new_1_type = 'wealth' then try_cast(COALESCE( ELEMENT_AT(SPLIT(items_new_1_converted_currency, '_') ,2),'0') as int) else 0 end + 
case when items_new_2_type = 'wealth' then try_cast(COALESCE( ELEMENT_AT(SPLIT(items_new_0_converted_currency, '_') ,2),'0') as int) else 0 end + 
case when items_new_3_type = 'wealth' then try_cast(COALESCE( ELEMENT_AT(SPLIT(items_new_1_converted_currency, '_') ,2),'0') as int) else 0 end + 
case when items_new_4_type = 'wealth' then try_cast(COALESCE( ELEMENT_AT(SPLIT(items_new_0_converted_currency, '_') ,2),'0') as int) else 0 end + 
case when items_new_5_type = 'wealth' then try_cast(COALESCE( ELEMENT_AT(SPLIT(items_new_5_converted_currency, '_') ,2),'0') as int) else 0 end + 
case when items_new_6_type = 'wealth' then try_cast(COALESCE( ELEMENT_AT(SPLIT(items_new_6_converted_currency, '_') ,2),'0') as int) else 0 end + 
case when items_new_7_type = 'wealth' then try_cast(COALESCE( ELEMENT_AT(SPLIT(items_new_7_converted_currency, '_') ,2),'0') as int) else 0 end + 
case when items_new_8_type = 'wealth' then try_cast(COALESCE( ELEMENT_AT(SPLIT(items_new_8_converted_currency, '_') ,2),'0') as int) else 0 end + 
case when items_new_9_type = 'wealth' then try_cast(COALESCE( ELEMENT_AT(SPLIT(items_new_9_converted_currency, '_') ,2),'0') as int) else 0 end + 
case when items_new_10_type = 'wealth' then try_cast(COALESCE( ELEMENT_AT(SPLIT(items_new_10_converted_currency, '_') ,2),'0') as int) else 0 end + 
case when items_new_11_type = 'wealth' then try_cast(COALESCE( ELEMENT_AT(SPLIT(items_new_11_converted_currency, '_') ,2),'0') as int) else 0 end + 
case when items_new_12_type = 'wealth' then try_cast(COALESCE( ELEMENT_AT(SPLIT(items_new_12_converted_currency, '_') ,2),'0') as int) else 0 end + 
case when items_new_13_type = 'wealth' then try_cast(COALESCE( ELEMENT_AT(SPLIT(items_new_13_converted_currency, '_') ,2),'0') as int) else 0 end + 
case when items_new_14_type = 'wealth' then try_cast(COALESCE( ELEMENT_AT(SPLIT(items_new_14_converted_currency, '_') ,2),'0') as int) else 0 end + 
case when items_new_15_type = 'wealth' then try_cast(COALESCE( ELEMENT_AT(SPLIT(items_new_15_converted_currency, '_') ,2),'0') as int) else 0 end +
case when items_new_16_type = 'wealth' then try_cast(COALESCE( ELEMENT_AT(SPLIT(items_new_16_converted_currency, '_') ,2),'0') as int) else 0 end + 
case when items_new_17_type = 'wealth' then try_cast(COALESCE( ELEMENT_AT(SPLIT(items_new_17_converted_currency, '_') ,2),'0') as int) else 0 end + 
case when items_new_18_type = 'wealth' then try_cast(COALESCE( ELEMENT_AT(SPLIT(items_new_18_converted_currency, '_') ,2),'0') as int) else 0 end + 
case when items_new_19_type = 'wealth' then try_cast(COALESCE( ELEMENT_AT(SPLIT(items_new_19_converted_currency, '_') ,2),'0') as int) else 0 end + 
case when items_new_20_type = 'wealth' then try_cast(COALESCE( ELEMENT_AT(SPLIT(items_new_20_converted_currency, '_') ,2),'0') as int) else 0 end + 
case when items_new_21_type = 'wealth' then try_cast(COALESCE( ELEMENT_AT(SPLIT(items_new_21_converted_currency, '_') ,2),'0') as int) else 0 end + 
case when items_new_22_type = 'wealth' then try_cast(COALESCE( ELEMENT_AT(SPLIT(items_new_22_converted_currency, '_') ,2),'0') as int) else 0 end + 
case when items_new_23_type = 'wealth' then try_cast(COALESCE( ELEMENT_AT(SPLIT(items_new_23_converted_currency, '_') ,2),'0') as int) else 0 end + 
case when items_new_24_type = 'wealth' then try_cast(COALESCE( ELEMENT_AT(SPLIT(items_new_24_converted_currency, '_') ,2),'0') as int) else 0 end + 
case when items_new_25_type = 'wealth' then try_cast(COALESCE( ELEMENT_AT(SPLIT(items_new_25_converted_currency, '_') ,2),'0') as int) else 0 end + 
case when items_new_26_type = 'wealth' then try_cast(COALESCE( ELEMENT_AT(SPLIT(items_new_26_converted_currency, '_') ,2),'0') as int) else 0 end + 
case when items_new_27_type = 'wealth' then try_cast(COALESCE( ELEMENT_AT(SPLIT(items_new_27_converted_currency, '_') ,2),'0') as int) else 0 end + 
case when items_new_28_type = 'wealth' then try_cast(COALESCE( ELEMENT_AT(SPLIT(items_new_28_converted_currency, '_') ,2),'0') as int) else 0 end + 
case when items_new_29_type = 'wealth' then try_cast(COALESCE( ELEMENT_AT(SPLIT(items_new_29_converted_currency, '_') ,2),'0') as int) else 0 end 
)as "Wealth_less_GBP"

from cte
group by dynamodb_keys_customer_id_s
