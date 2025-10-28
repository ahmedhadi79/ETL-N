###########################################################
# AWS Lambda function: Banking activity
###########################################################
module "lambda_customer_detail_to_s3_curated" {
  source  = "terraform-aws-modules/lambda/aws"
  version = "7.9.0"

  function_name = "${local.prefix}-customer-detail-to-s3-curated"
  handler       = "lambda_function.lambda_handler"
  runtime       = "python3.12"
  timeout       = 900
  layers        = [local.lambda_layer_aws_wrangler_arn]
  memory_size   = 10240

  source_path = [
    "${path.module}/../src/lambdas/customer_detail_to_s3_curated",
  ]

  environment_variables = {
    S3_CURATED = local.curated_datalake_bucket_name
    IS_SANDBOX = var.bespoke_account == "sandbox" ? "true" : "false"
  }

  hash_extra   = "${local.prefix}-customer-detail-to-s3-curated"
  create_role  = false
  lambda_role  = aws_iam_role.iam_for_lambda.arn
  tracing_mode = "Active"

  recreate_missing_package = false
  ignore_source_code_hash  = true
}

###########################################################
# AWS Event Bridge Rule
###########################################################

resource "aws_cloudwatch_event_rule" "schedule_customer_detail" {
  name                = module.lambda_customer_detail_to_s3_curated.lambda_function_name
  description         = "Schedule Lambda function execution for Customer Detail"
  schedule_expression = "cron(0 0/8 * * ? *)"
}

resource "aws_cloudwatch_event_target" "customer_detail_lambdaexecution" {
  arn  = module.lambda_customer_detail_to_s3_curated.lambda_function_arn
  rule = aws_cloudwatch_event_rule.schedule_customer_detail.name
}

###########################################################
# AWS Lambda Trigger
###########################################################
resource "aws_lambda_permission" "customer_detail_allow_cloudwatch_event_rule" {
  statement_id  = "AllowExecutionFromCloudWatch"
  action        = "lambda:InvokeFunction"
  function_name = module.lambda_customer_detail_to_s3_curated.lambda_function_arn
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.schedule_customer_detail.arn
}
