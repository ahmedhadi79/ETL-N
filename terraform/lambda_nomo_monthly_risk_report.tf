###########################################################
# AWS Lambda function: Nomo Monthly Risk Repost
###########################################################
module "lambda_nomomonthly_riskreport" {
  source  = "terraform-aws-modules/lambda/aws"
  version = "7.9.0"

  function_name = "${local.prefix}-nomo-monthly-risk-report"
  handler       = "lambda_function.lambda_handler"
  runtime       = "python3.12"
  timeout       = 900
  layers        = [local.lambda_layer_aws_wrangler_arn]
  memory_size   = 10240

  source_path = [
    "${path.module}/../src/lambdas/nomo_monthly_risk_report",
  ]

  environment_variables = {
    ENV_NAME       = var.bespoke_account,
    SES_FROM_EMAIL = "donotreplay@${var.bespoke_account}.bb2bank.io",
    S3_CURATED     = local.curated_datalake_bucket_name
  }

  hash_extra   = "${local.prefix}-nomo-monthly-risk-report"
  create_role  = false
  lambda_role  = aws_iam_role.iam_for_lambda.arn
  tracing_mode = "Active"

  recreate_missing_package = false
  ignore_source_code_hash  = true
}

###########################################################
# AWS Event Bridge Rule
###########################################################

resource "aws_cloudwatch_event_rule" "schedule_monthly_nomo_report_sender" {
  name                = module.lambda_nomomonthly_riskreport.lambda_function_name
  description         = "Schedule Lambda function execution from Monthly Nomo Report"
  schedule_expression = "cron(30 10 1 * ? *)"
}

resource "aws_cloudwatch_event_target" "monthly_nomo_report_lambdaexecution" {
  arn  = module.lambda_nomomonthly_riskreport.lambda_function_arn
  rule = aws_cloudwatch_event_rule.schedule_monthly_nomo_report_sender.name
}

###########################################################
# AWS Lambda Trigger
###########################################################
resource "aws_lambda_permission" "monthly_nomo_report_allow_cloudwatch_event_rule" {
  statement_id  = "AllowExecutionFromCloudWatch"
  action        = "lambda:InvokeFunction"
  function_name = module.lambda_nomomonthly_riskreport.lambda_function_arn
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.schedule_monthly_nomo_report_sender.arn
}
