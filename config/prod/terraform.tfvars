### Common
aws_account_id         = "187003861892" # prod
logging_aws_account_id = "295970363876" # logging
bespoke_account        = "prod"


s3_bucket_adjust_logs = "adjust-logs-d0a3da9b"


### Customer Lambda  # TODO: non-consistent data between envs, need to check
lambda_customer_risk_form_memory_size          = 10240
lambda_customer_risk_form_timeout              = 900
lambda_customer_riskscore_memory_size          = 10240
lambda_customer_riskscore_timeout              = 900
lambda_customer_wealth_income_data_memory_size = 10240
lambda_customer_wealth_income_data_timeout     = 900

### sfmc-data
sfmc_bucket = "sfmc-data-collection-d0a3da9b" # TODO: non-consistent data, need to check

#currency cloud variable
slscurrencycloud_auth_details       = "data/currencycloud/api" # TODO: rewrite this with parameters etc.
lambda_slscurrencycloud_to_s3_state = "DISABLED"

### cards-paymentology
s3_sftp_paymentology_bucket = "sftp-replica-8435bcc1" # TODO: rewrite this with parameters etc.

### partnership reporting to s3 raw
lambda_partnership_reporting_to_curated_state = "DISABLED"

### Temp deposit accounts backfill data
lambda_temp_deposit_accounts_memory_size = 10240

### BBYN risk report
url_currency_code = "sls/url/currencyCode"

## OUTDATED/DISABLED
### Quicksight Authors Lambda
lambda_quicksight_authors_datasets_memory_size = 10240
lambda_quicksight_authors_datasets_timeout     = 900

### Clear Bank
cb_main_account_id = "0033761c-700c-4797-923e-dc92fcad0d27"
cb_base_url        = "https://institution-api.clearbank.co.uk/v2/"

datalake_imal_reporting_to_s3_raw_enabled = "false"
lambda_cb_transactions_tos3raw_enable = "ENABLED"
lambda_cb_directdebit_mandates_tos3raw_enable = "ENABLED"

amplitude_table_name = "338308"

allfunds_base_url = "https://nextportfolio.allfunds.com"
lambda_allfund_cron_enable                   = "ENABLED"

lambda_log_level = "ERROR"

