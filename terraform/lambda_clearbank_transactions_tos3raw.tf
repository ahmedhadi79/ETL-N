##########################################################
# AWS Lambda function: CB transactions to S3 Raw
###########################################################
module "lambda_cb_transactions_tos3raw" {
  source  = "terraform-aws-modules/lambda/aws"
  version = "7.9.0"

  function_name = "${local.prefix}-cb-transactions-tos3raw"
  handler       = "lambda_function.lambda_handler"
  runtime       = "python3.12"
  timeout       = 900
  memory_size   = 10240
  layers        = [local.lambda_layer_aws_wrangler_arn]

  source_path = [
    "${path.module}/../src/common/api_client.py",
    {
      path             = "${path.module}/../src/lambdas/clearbank_transactions_to_s3_raw",
      pip_requirements = true,
      patterns = [
        "!requirements.txt",
        "!tests/.*",
        "!unittest/.*",
        "!backfill/.*",
        "!.DS_Store",
        "!.*.md",
        "!.package/.*",
      ]
    }
  ]

  environment_variables = {
    ENV              = var.bespoke_account,
    S3_RAW           = local.raw_datalake_bucket_name,
    CB_API_KEY       = var.cb_api_key_details,
    CB_AUTH_DETAILS  = var.cb_auth_details,
    CB_BASE_URL      = var.cb_base_url,
    MAIN_ACCOUNT_ID  = var.cb_main_account_id,
    CB_TABLE         = "Transactions"
    CB_FILTER_OBJECT = "transactions"
  }

  hash_extra   = "${local.prefix}-cb-transactions-tos3raw"
  create_role  = false
  lambda_role  = aws_iam_role.iam_for_lambda.arn
  tracing_mode = "Active"

  recreate_missing_package = false
  ignore_source_code_hash  = true
}

###########################################################
# AWS Event Bridge Rule
###########################################################
resource "aws_cloudwatch_event_rule" "schedule_cb_transactions_tos3raw" {
  name                = module.lambda_cb_transactions_tos3raw.lambda_function_name
  description         = "Schedule Lambda function execution from CB transactions to S3"
  schedule_expression = "cron(30 00 * * ? *)"
  state               = var.lambda_cb_transactions_tos3raw_enable
}

resource "aws_cloudwatch_event_target" "cb_transactions_tos3raw_lambdaexecution" {
  arn  = module.lambda_cb_transactions_tos3raw.lambda_function_arn
  rule = aws_cloudwatch_event_rule.schedule_cb_transactions_tos3raw.name
}

###########################################################
# AWS Lambda Trigger
###########################################################
resource "aws_lambda_permission" "cb_transactions_tos3raw_allow_cloudwatch_event_rule" {
  statement_id  = "AllowExecutionFromCloudWatch"
  action        = "lambda:InvokeFunction"
  function_name = module.lambda_cb_transactions_tos3raw.lambda_function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.schedule_cb_transactions_tos3raw.arn
}
