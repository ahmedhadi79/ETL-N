# ###########################################################
# # AWS Lambda function: Currency Cloud Data to S3 Raw
# ###########################################################
module "lambda_currency_cloud_data_to_s3" {
  source  = "terraform-aws-modules/lambda/aws"
  version = "7.9.0"

  function_name = "${local.prefix}-currency-cloud-api"
  handler       = "lambda_function.lambda_handler"
  runtime       = "python3.12"
  timeout       = 900
  layers        = [local.lambda_layer_aws_wrangler_arn]
  memory_size   = 10240

  source_path = [
    {
      path             = "${path.module}/../src/lambdas/currency_cloud_data_to_s3_raw",
      pip_requirements = true,
      patterns = [
        "!tests/.*",
        "!backfill/.*",
        "!.DS_Store",
        "!.*.md",
        "!.package/.*",
      ]
    },
  ]

  environment_variables = {
    S3_RAW = local.raw_datalake_bucket_name
  }

  hash_extra   = "${local.prefix}-currency-cloud-api"
  create_role  = false
  lambda_role  = aws_iam_role.iam_for_lambda.arn
  tracing_mode = "Active"

  recreate_missing_package = false
  ignore_source_code_hash  = true
}

###########################################################
# AWS Event Bridge Rule
###########################################################

resource "aws_cloudwatch_event_rule" "schedulecurrencycloudtos3" {
  name                = module.lambda_currency_cloud_data_to_s3.lambda_function_name
  description         = "Schedule Lambda function execution from Salesforce to S3"
  schedule_expression = "cron(0 07 * * ? *)"
  state               = var.lambda_slscurrencycloud_to_s3_state
}

resource "aws_cloudwatch_event_target" "currencycloudtos3_lambdaexecution" {
  arn  = module.lambda_currency_cloud_data_to_s3.lambda_function_arn
  rule = aws_cloudwatch_event_rule.schedulecurrencycloudtos3.name
}

###########################################################
# AWS Lambda Trigger
###########################################################
resource "aws_lambda_permission" "currencycloudtos3_allow_cloudwatch_event_rule" {
  statement_id  = "AllowExecutionFromCloudWatch"
  action        = "lambda:InvokeFunction"
  function_name = module.lambda_currency_cloud_data_to_s3.lambda_function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.schedulecurrencycloudtos3.arn
}
