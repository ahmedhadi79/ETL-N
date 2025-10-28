###########################################################
# AWS Lambda function: SFTP to S3 Raw
###########################################################
module "lambda_cardspaymentologytos3" {
  source  = "terraform-aws-modules/lambda/aws"
  version = "7.9.0"

  function_name = "${local.prefix}-cards-paymentology-to-s3-raw"
  handler       = "lambda_function.lambda_handler"
  runtime       = "python3.12"
  timeout       = 900
  layers        = [local.lambda_layer_aws_wrangler_arn]
  memory_size   = 2048

  source_path = [
    "${path.module}/../src/lambdas/cards_paymentology_to_s3_raw",
  ]

  environment_variables = {
    S3_RAW         = local.raw_datalake_bucket_name,
    SFTP_S3_BUCKET = var.s3_sftp_paymentology_bucket,
    SFTP_S3_KEY    = "${var.bespoke_account}-paymentology",
  }

  hash_extra   = "${local.prefix}-cards-paymentology-to-s3-raw"
  create_role  = false
  lambda_role  = aws_iam_role.iam_for_lambda.arn
  tracing_mode = "Active"

  recreate_missing_package = false
  ignore_source_code_hash  = true
}

###########################################################
# AWS Event Bridge Rule
###########################################################

resource "aws_cloudwatch_event_rule" "schedule_cardspaymentologytos3" {
  name                = "datalake-cardspaymentologytos3-cloudwatch-rule"
  description         = "Schedule Lambda function execution from SFTP to S3"
  schedule_expression = "cron(10 4 ? * MON-FRI *)"
}

resource "aws_cloudwatch_event_target" "cardspaymentologytos3_lambdaexecution" {
  arn  = module.lambda_cardspaymentologytos3.lambda_function_arn
  rule = aws_cloudwatch_event_rule.schedule_cardspaymentologytos3.name
}

###########################################################
# AWS Lambda Trigger
###########################################################
resource "aws_lambda_permission" "cardspaymentologytos3_allow_cloudwatch_event_rule" {
  statement_id  = "AllowExecutionFromCloudWatch"
  action        = "lambda:InvokeFunction"
  function_name = module.lambda_cardspaymentologytos3.lambda_function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.schedule_cardspaymentologytos3.arn
}
