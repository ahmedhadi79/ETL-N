###########################################################
# AWS Lambda function: customer_wealth_income_data
###########################################################
module "lambda_customer_wealth_income_data" {
  source  = "terraform-aws-modules/lambda/aws"
  version = "7.9.0"

  function_name = "${local.prefix}-customer-wealth-income-data"
  handler       = "lambda_function.lambda_handler"
  runtime       = "python3.12"
  timeout       = var.lambda_customer_wealth_income_data_timeout
  memory_size   = var.lambda_customer_wealth_income_data_memory_size
  layers        = [local.lambda_layer_aws_wrangler_arn]

  source_path = [
    {
      path = "${path.module}/../src/lambdas/customer_wealth_income_data",
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
    S3_RAW = var.sfmc_bucket,
  }

  hash_extra               = "${local.prefix}-customer-wealth-income-data"
  create_role              = false
  lambda_role              = aws_iam_role.iam_for_lambda.arn
  tracing_mode             = "Active"
  recreate_missing_package = false
  ignore_source_code_hash  = true

}

###########################################################
# AWS Event Bridge Rule it ill be triggered every week 10 am 
###########################################################

resource "aws_cloudwatch_event_rule" "schedule_customer_wealth_income_data" {
  name                = module.lambda_customer_wealth_income_data.lambda_function_name
  description         = "Schedule Lambda function execution for Customer Captured Changes"
  schedule_expression = "cron(0 10 ? * MON *)"
}

resource "aws_cloudwatch_event_target" "customer_wealth_income_lambdaexecution" {
  arn  = module.lambda_customer_wealth_income_data.lambda_function_arn
  rule = aws_cloudwatch_event_rule.schedule_customer_wealth_income_data.name
}

###########################################################
# AWS Lambda Trigger
###########################################################
resource "aws_lambda_permission" "customer_wealth_income_cloudwatch_event_rule" {
  statement_id  = "AllowExecutionFromCloudWatch"
  action        = "lambda:InvokeFunction"
  function_name = module.lambda_customer_wealth_income_data.lambda_function_arn
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.schedule_customer_wealth_income_data.arn
}
