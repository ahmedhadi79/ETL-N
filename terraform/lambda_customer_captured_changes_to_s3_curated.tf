###########################################################
# AWS Lambda function: Banking activity
###########################################################
module "lambda_customer_captured_changes_to_s3_curated" {
  source  = "terraform-aws-modules/lambda/aws"
  version = "7.9.0"

  function_name = "${local.prefix}-customer-captured-changes-to-s3-curated"
  handler       = "lambda_function.lambda_handler"
  runtime       = "python3.12"
  timeout       = 900
  layers        = [local.lambda_layer_aws_wrangler_arn]
  memory_size   = 10240

  source_path = [
    {
      path = "${path.module}/../src/lambdas/customer_captured_changes_to_s3_curated",
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
    S3_CURATED = local.curated_datalake_bucket_name
    IS_SANDBOX = var.bespoke_account == "sandbox" ? "true" : "false"
  }

  hash_extra   = "${local.prefix}-customer-captured-changes-to-s3-curated"
  create_role  = false
  lambda_role  = aws_iam_role.iam_for_lambda.arn
  tracing_mode = "Active"

  recreate_missing_package = false
  ignore_source_code_hash  = true
}

###########################################################
# AWS Event Bridge Rule
###########################################################

resource "aws_cloudwatch_event_rule" "schedule_customer_captured_changes" {
  name                = module.lambda_customer_captured_changes_to_s3_curated.lambda_function_name
  description         = "Schedule Lambda function execution for Customer Captured Changes"
  schedule_expression = "cron(0 2,8,14,20 * * ? *)"
}

resource "aws_cloudwatch_event_target" "customer_captured_changes_lambdaexecution" {
  arn  = module.lambda_customer_captured_changes_to_s3_curated.lambda_function_arn
  rule = aws_cloudwatch_event_rule.schedule_customer_captured_changes.name
}

###########################################################
# AWS Lambda Trigger
###########################################################
resource "aws_lambda_permission" "customer_captured_changes_allow_cloudwatch_event_rule" {
  statement_id  = "AllowExecutionFromCloudWatch"
  action        = "lambda:InvokeFunction"
  function_name = module.lambda_customer_captured_changes_to_s3_curated.lambda_function_arn
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.schedule_customer_captured_changes.arn
}
