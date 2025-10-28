### Common
aws_account_id         = "414717704904" # sandbox
logging_aws_account_id = "295970363876" # logging
bespoke_account        = "sandbox"

s3_bucket_adjust_logs = "adjust-logs-d6bba652"

### Customer # TODO: non-consistent data between envs, need to check
lambda_customer_risk_form_memory_size          = 512
lambda_customer_risk_form_timeout              = 180
lambda_customer_riskscore_memory_size          = 512
lambda_customer_riskscore_timeout              = 180
lambda_customer_wealth_income_data_memory_size = 512
lambda_customer_wealth_income_data_timeout     = 180

### sfmc-data
sfmc_bucket = "bb2-sandbox-datalake-curated" # TODO: non-consistent data, need to check

#currency cloud variable
slscurrencycloud_auth_details = "data/currencycloud/api" # TODO: rewrite this with parameters etc.

### cards-paymentology
s3_sftp_paymentology_bucket = "sftp-replica-98abd2e9"

### Temp deposit accounts backfill data
lambda_temp_deposit_accounts_memory_size = 256

### BBYN risk report
url_currency_code = "sls/url/currencyCode"

## OUTDATED/DISABLED
### Quicksight Authors Lambda
lambda_quicksight_authors_datasets_memory_size = 256
lambda_quicksight_authors_datasets_timeout     = 600

### Clear Bank
cb_main_account_id = "a4f5d7cf-83ab-4690-a029-9016ff275095"
cb_base_url        = "https://institution-api-sim.clearbank.co.uk/v2/"

amplitude_table_name = "339441"

allfunds_base_url = "https://test.v2.nextportfolio.com"

lambda_log_level = "DEBUG"
