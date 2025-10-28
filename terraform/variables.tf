# global vars
variable "region" {
  type        = string
  default     = "eu-west-2"
  description = "AWS Region the S3 bucket should reside in"
}

variable "bespoke_account" {
  description = "bespoke account to deploy (sandbox, nfrt, alpha, beta, production)"
  type        = string
}

variable "logging_aws_account_id" {
  type        = string
  description = "Logging AWS Account ID which may be operated on by this template"
}

variable "logging_external_id" {
  description = "External identifier to use when assuming the role in the Logging account."
  type        = string
}

variable "aws_account_id" {
  type        = string
  description = "AWS Account ID which may be operated on by this template"
}

variable "external_id" {
  description = "External identifier to use when assuming the role."
  type        = string
}

variable "resource_management_iam_role" {
  description = "Name of the role TF uses to manage resources in AWS accounts."
  type        = string
}

variable "project_url" {
  description = "URL of the gitlab project that owns the resources"
  default     = "http://localhost"
  type        = string
}

variable "lambda_quicksight_authors_datasets_memory_size" {
  type        = number
  description = "Memory size for this lambda function."
}

variable "lambda_quicksight_authors_datasets_timeout" {
  type        = number
  description = "Timeout for this lambda function."
}

variable "sfmc_bucket" {
  description = "SFMC Bucket name"
  type        = string
}

variable "slscurrencycloud_auth_details" {
  type        = string
  description = "slscurrencycloud Authentication"
}

variable "lambda_slscurrencycloud_to_s3_state" {
  type        = string
  description = "To enable or not this function"
  default     = "ENABLED"
}

variable "s3_sftp_paymentology_bucket" {
  type        = string
  description = "The name of the s3 bucket for sync sftp data"
}

########################## customer-risk-form-data-raw-curated ######################
variable "lambda_customer_risk_form_memory_size" {
  type        = number
  description = "Memory size for this lambda function."
}

variable "lambda_customer_risk_form_timeout" {
  type        = number
  description = "Timeout for this lambda function."
}

########################## partnership-reporting-to-curated ######################
variable "lambda_partnership_reporting_to_curated_state" {
  type        = string
  description = "To enable or not this function"
  default     = "ENABLED"
}

########################################
variable "lambda_customer_riskscore_memory_size" {
  type        = number
  description = "Memory size for this lambda function."
}

variable "lambda_customer_riskscore_timeout" {
  type        = number
  description = "Timeout for this lambda function."
}

########################################

variable "lambda_customer_wealth_income_data_timeout" {
  type        = number
  description = "Timeout for this lambda function."
}

variable "lambda_customer_wealth_income_data_memory_size" {
  type        = number
  description = "Memory size for this lambda function."
}

variable "s3_bucket_adjust_logs" {
  description = "Name for the S3 bucket for adjust logs"
  type        = string
}


###########Temp deposit account backfill data ###########
variable "lambda_temp_deposit_accounts_memory_size" {
  type        = number
  description = "Memory size for this lambda function."
}

#################################################
variable "url_currency_code" {
  type        = string
  description = "BBYN currency code url"
}

###########Clear Bank ###########
variable "cb_auth_details" {
  type        = string
  description = "Clear Bank Authentication"
  default     = "sls/clearbank"
}

###########Clear Bank API Key###########
variable "cb_api_key_details" {
  type        = string
  description = "Clear Bank API Key Authentication"
  default     = "sls/clearbank/clearbankApiKey"
}

variable "cb_base_url" {
  type        = string
  description = "Clear bank login URL"
}

variable "cb_main_account_id" {
  type        = string
  description = "Clear bank login URL"
}

variable "datalake_imal_reporting_to_s3_raw_enabled" {
  type        = string
  description = "Glue trigger enabled bool"
  default     = "false"
}

variable "lambda_cb_transactions_tos3raw_enable" {
  type        = string
  description = "Event bridge state rule"
  default     = "DISABLED"
}

variable "lambda_cb_directdebit_mandates_tos3raw_enable" {
  type        = string
  description = "Event bridge state rule"
  default     = "DISABLED"
}

variable "amplitude_table_name" {
  description = "Name for the S3 bucket for Glue scripts"
  type        = string
}

variable "lambda_allfund_cron_enable" {
  type        = string
  description = "Event bridge state rule"
  default     = "ENABLED"
}

variable "allfunds_base_url" {
  description = "Name for the allfunds_base_url"
  type        = string
}

variable "allfunds_auth_path" {
  description = "Name for the allfunds_auth_path"
  type        = string
  default     = "sls/data/allFundsReadOnly"
}

variable "lambda_log_level" {
  description = "Log level for Lambda functions (DEBUG, INFO, WARNING, ERROR, CRITICAL)"
  type        = string
  default     = "ERROR"
}
