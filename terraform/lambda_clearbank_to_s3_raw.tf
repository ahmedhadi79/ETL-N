##########################################################
# AWS Lambda function: ClearBank to S3 Raw
###########################################################
module "lambda_clearbank_to_s3_raw" {
  source  = "terraform-aws-modules/lambda/aws"
  version = "7.9.0"

  function_name = "${local.prefix}-clearbank-to-s3-raw"
  handler       = "lambda_function.lambda_handler"
  runtime       = "python3.12"
  timeout       = 900
  memory_size   = 10240
  layers        = [local.lambda_layer_aws_wrangler_arn]
  tracing_mode  = "Active"

  source_path = [
    "${path.module}/../src/common/api_client.py",
    {
      path             = "${path.module}/../src/lambdas/clearbank_to_s3_raw",
      pip_requirements = true,
      patterns = [
        "!requirements.txt",
        "!tests/.*",
        "!unittest/.*",
        "!.DS_Store",
        "!.*.md",
        "!.package/.*",
      ]
    }
  ]

  environment_variables = {
    ENV             = var.bespoke_account
    S3_RAW          = local.raw_datalake_bucket_name
    CB_API_KEY      = var.cb_api_key_details
    CB_AUTH_DETAILS = var.cb_auth_details
    CB_BASE_URL     = var.cb_base_url
    MAIN_ACCOUNT_ID = var.cb_main_account_id

    # Optional global defaults (can be overridden by EventBridge input)
    PAGE_SIZE  = "1000"
    BATCH_SIZE = "50"
  }

  hash_extra               = "${local.prefix}-clearbank-to-s3-raw"
  create_role              = false
  lambda_role              = aws_iam_role.iam_for_lambda.arn
  recreate_missing_package = false
  ignore_source_code_hash  = true
}

###########################################################
# AWS Event Bridge Rule Mandates Delta
###########################################################
resource "aws_cloudwatch_event_rule" "schedule_cb_mandates_delta" {
  name                = "${local.prefix}-cb-mandates-delta-schedule"
  description         = "Schedule: ClearBank mandates delta -> S3 RAW"
  schedule_expression = "cron(00 01 * * ? *)"
  state               = var.lambda_cb_directdebit_mandates_tos3raw_enable
}

resource "aws_cloudwatch_event_target" "cb_mandates_delta_target" {
  arn  = module.lambda_clearbank_to_s3_raw.lambda_function_arn
  rule = aws_cloudwatch_event_rule.schedule_cb_mandates_delta.name

  input = jsonencode({
    job_type   = "mandates_delta",
    page_size  = 1000,
    batch_size = 50
    # You can pass custom windows too if needed:
    # start_date = "2025-10-21",
    # end_date   = "2025-10-22"
  })
}

# AWS Lambda Trigger
resource "aws_lambda_permission" "cb_mandates_delta_allow_events" {
  statement_id  = "AllowExecutionFromCloudWatchMandatesDelta"
  action        = "lambda:InvokeFunction"
  function_name = module.lambda_clearbank_to_s3_raw.lambda_function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.schedule_cb_mandates_delta.arn
}

###########################################################
# AWS Event Bridge Rule Transactions Daily
###########################################################
resource "aws_cloudwatch_event_rule" "schedule_cb_transactions_daily" {
  name                = "${local.prefix}-cb-transactions-daily-schedule"
  description         = "Schedule: ClearBank transactions daily -> S3 RAW"
  schedule_expression = "cron(30 00 * * ? *)"
  state               = var.lambda_cb_transactions_tos3raw_enable
}

resource "aws_cloudwatch_event_target" "cb_transactions_daily_target" {
  arn  = module.lambda_clearbank_to_s3_raw.lambda_function_arn
  rule = aws_cloudwatch_event_rule.schedule_cb_transactions_daily.name

  input = jsonencode({
    job_type         = "transactions_daily",
    cb_table         = "Transactions",
    cb_filter_object = "transactions",
    page_size        = 1000
    # Optional to override default: start_date/end_date (UTC "yesterday" is default in code)
    # start_date = "2025-10-21",
    # end_date   = "2025-10-21"
  })
}

# AWS Lambda Trigger
resource "aws_lambda_permission" "cb_transactions_daily_allow_events" {
  statement_id  = "AllowExecutionFromCloudWatchTransactionsDaily"
  action        = "lambda:InvokeFunction"
  function_name = module.lambda_clearbank_to_s3_raw.lambda_function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.schedule_cb_transactions_daily.arn
}
