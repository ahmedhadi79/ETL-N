###########################################################
# AWS Lambda function: Partnership reporting to curated
###########################################################
module "lambda_partnership_reporting_to_curated" {
  source  = "terraform-aws-modules/lambda/aws"
  version = "7.9.0"

  function_name = "${local.prefix}-partnership-reporting-to-curated"
  handler       = "lambda_function.lambda_handler"
  runtime       = "python3.12"
  timeout       = "600"
  layers        = [local.lambda_layer_aws_wrangler_arn]
  memory_size   = 6144

  source_path = [
    "${path.module}/../src/lambdas/partnership_reporting_to_curated",
  ]

  environment_variables = {
    S3_CURATED = local.curated_datalake_bucket_name,
    S3_ATHENA  = local.athena_results_bucket_name,
    DATABASE   = "datalake_curated"
  }

  hash_extra   = "${local.prefix}-partnership-reporting-to-curated"
  create_role  = false
  lambda_role  = aws_iam_role.iam_for_lambda.arn
  tracing_mode = "Active"

  recreate_missing_package = false
  ignore_source_code_hash  = true
}

###########################################################
# AWS Event Bridge Rule
###########################################################

resource "aws_cloudwatch_event_rule" "schedule_partnership_reporting_to_curated" {
  name                = module.lambda_partnership_reporting_to_curated.lambda_function_name
  description         = "Schedule Lambda function execution for partnership reporting lambda to curated"
  schedule_expression = "cron(0 16 * * ? *)"
  state               = var.lambda_partnership_reporting_to_curated_state
}

resource "aws_cloudwatch_event_target" "partnership_reporting_to_curated_lambdaexecution" {
  arn  = module.lambda_partnership_reporting_to_curated.lambda_function_arn
  rule = aws_cloudwatch_event_rule.schedule_partnership_reporting_to_curated.name
}

###########################################################
# AWS Lambda Trigger
###########################################################
resource "aws_lambda_permission" "partnership_reporting_to_curated_allow_cloudwatch_event_rule" {
  statement_id  = "AllowExecutionFromCloudWatch"
  action        = "lambda:InvokeFunction"
  function_name = module.lambda_partnership_reporting_to_curated.lambda_function_arn
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.schedule_partnership_reporting_to_curated.arn
}
