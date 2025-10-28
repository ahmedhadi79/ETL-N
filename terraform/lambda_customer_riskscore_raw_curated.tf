###########################################################
# AWS Lambda function: customer_risk_form_date
###########################################################
module "lambda_customer_riskscore_data_to_s3_curated" {
  source  = "terraform-aws-modules/lambda/aws"
  version = "7.9.0"

  function_name = "${local.prefix}-customer-risk-score-data-to-s3-curated"
  handler       = "lambda_function.lambda_handler"
  runtime       = "python3.12"
  timeout       = var.lambda_customer_riskscore_timeout
  layers        = [local.lambda_layer_aws_wrangler_arn]
  memory_size   = var.lambda_customer_riskscore_memory_size

  source_path = [
    {
      path = "${path.module}/../src/lambdas/customer_risk_score_raw_to_curated",
      patterns = [
        "!tests/.*",
        "!unittest/.*",
        "!backfill/.*",
        "!.DS_Store",
        "!.*.md",
        "!.package/.*",
      ]
    },
  ]

  environment_variables = {
    WRANGLER_WRITE_MODE = "overwrite_partitions"
    S3_CURATED          = local.curated_datalake_bucket_name
  }

  hash_extra   = "${local.prefix}-customer-risk-score-data-to-s3-curated"
  create_role  = false
  lambda_role  = aws_iam_role.iam_for_lambda.arn
  tracing_mode = "Active"

  recreate_missing_package = false
  ignore_source_code_hash  = true
}

###########################################################
# AWS Event Bridge Rule
###########################################################

resource "aws_cloudwatch_event_rule" "schedule_customer_riskscore_form" {
  name                = module.lambda_customer_riskscore_data_to_s3_curated.lambda_function_name
  description         = "Schedule Lambda function execution for moving customer risk for data to cursted database "
  schedule_expression = "cron(0 01 * * ? *)"
}

resource "aws_cloudwatch_event_target" "schedule_customer_riskscore_form_lambdaexecution" {
  arn  = module.lambda_customer_riskscore_data_to_s3_curated.lambda_function_arn
  rule = aws_cloudwatch_event_rule.schedule_customer_riskscore_form.name
}

###########################################################
# AWS Lambda Trigger
###########################################################
resource "aws_lambda_permission" "schedule_customer_riskscore_form_allow_cloudwatch_event_rule" {
  statement_id  = "AllowExecutionFromCloudWatch"
  action        = "lambda:InvokeFunction"
  function_name = module.lambda_customer_riskscore_data_to_s3_curated.lambda_function_arn
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.schedule_customer_riskscore_form.arn
}
