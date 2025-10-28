resource "aws_s3_object" "datalake_cb_virtual_accounts_tos3raw" {
  bucket = local.glue_assets_bucket_name
  key    = "${local.project_name}/scripts/clearbank_data_to_s3_raw.py"
  source = "../src/glue/clearbank_data_to_s3_raw.py"

  etag = filemd5("../src/glue/clearbank_data_to_s3_raw.py")
}

resource "aws_s3_object" "datalake_cb_virtual_accounts_tos3raw_data_catalog" {
  bucket = local.glue_assets_bucket_name
  key    = "${local.project_name}/scripts/clearbank_virtual_accounts_to_s3_raw/data_catalog.py"
  source = "../src/lambdas/clearbank_virtual_accounts_to_s3_raw/data_catalog.py"

  etag = filemd5("../src/lambdas/clearbank_virtual_accounts_to_s3_raw/data_catalog.py")
}


resource "aws_glue_job" "datalake_cb_virtual_accounts_tos3raw" {
  name        = "${local.prefix}-clearbank-virtual-accounts-tos3raw"
  role_arn    = aws_iam_role.iam_for_clearbank_glue_etl.arn
  max_retries = "0"
  timeout     = 120
  command {
    name            = "pythonshell"
    script_location = "s3://${local.glue_assets_bucket_name}/${aws_s3_object.datalake_cb_virtual_accounts_tos3raw.key}"
    python_version  = 3.9
  }
  execution_property {
    max_concurrent_runs = 100
  }
  default_arguments = {
    "--enable-auto-scaling"              = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                   = "true"
    "--TempDir"                          = "s3://${local.glue_assets_bucket_name}/temporary/"                         
   "--extra-py-files" = join(",", [
      "s3://${local.glue_assets_bucket_name}/${aws_s3_object.glue_api_client.key}",
      "s3://${local.glue_assets_bucket_name}/${aws_s3_object.glue_custom_functions.key}",
       "s3://${local.glue_assets_bucket_name}/${aws_s3_object.datalake_cb_virtual_accounts_tos3raw_data_catalog.key}",
    ])
    "--enable-glue-datacatalog"          = "true"
    "--ENV"                              = var.bespoke_account
    "--S3_RAW"                           = local.raw_datalake_bucket_name
    "--additional-python-modules"        = "flatten_json==0.1.14"
    "--ENV"                              = var.bespoke_account
    "--CB_API_KEY"                       = var.cb_api_key_details
    "--CB_AUTH_DETAILS"                  = var.cb_auth_details
    "--CB_BASE_URL"                      = var.cb_base_url
    "--MAIN_ACCOUNT_ID"                  = var.cb_main_account_id
    "--CB_TABLE"                         = "Virtual"
    "--CB_FILTER_OBJECT"                 = "accounts"
    "library-set"                        = "analytics"
  }
  max_capacity = 1
}

###########################################################
# AWS Glue Triggers - clearbank virtual accounts
###########################################################
resource "aws_glue_trigger" "datalake_cb_virtual_accounts_trigger" {
  name     = "datalake_cb_virtual_accounts_trigger"
  schedule = "cron(30 00 * * ? *)"
  type     = "SCHEDULED"
  enabled  = "true"

  actions {
    job_name = aws_glue_job.datalake_cb_virtual_accounts_tos3raw.name
  }
}

