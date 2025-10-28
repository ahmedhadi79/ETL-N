### Common
aws_account_id         = "466535336611" # beta
logging_aws_account_id = "295970363876" # logging
bespoke_account        = "beta"

s3_bucket_adjust_logs           = "adjust-logs-31e12e43"

### Customer # TODO: non-consistent data between envs, need to check
lambda_customer_risk_form_memory_size          = 512
lambda_customer_risk_form_timeout              = 180
lambda_customer_riskscore_memory_size          = 512
lambda_customer_riskscore_timeout              = 180
lambda_customer_wealth_income_data_memory_size = 512
lambda_customer_wealth_income_data_timeout     = 180

### sfmc-data
sfmc_bucket = "bb2-beta-datalake-curated" # TODO: non-consistent data, need to check

#currency cloud variable
slscurrencycloud_auth_details         = "data/currencycloud/api" # TODO: rewrite this with parameters etc.

### cards-paymentology
s3_sftp_paymentology_bucket           = "sftp-replica-de587e1a"

### Temp deposit accounts backfill data
lambda_temp_deposit_accounts_memory_size = 256

### BBYN risk report
url_currency_code = "sls/url/currencyCode"

## OUTDATED/DISABLED
### Quicksight Authors Lambda
lambda_quicksight_authors_datasets_memory_size = 256
lambda_quicksight_authors_datasets_timeout     = 900

### Clear Bank
cb_main_account_id = "b5e6dd1f-f200-438c-a726-a27d1354f5c9"
cb_base_url = "https://institution-api-sim.clearbank.co.uk/v2/"

amplitude_table_name = "338520"

allfunds_base_url = "https://test.v2.nextportfolio.com"

lambda_log_level = "DEBUG"
