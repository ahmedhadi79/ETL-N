###########################################################
# AWS Lambda function: SFTP to S3 Raw
###########################################################
module "lambda_imalreportingtos3" {
  source  = "terraform-aws-modules/lambda/aws"
  version = "7.9.0"

  function_name = "datalake-${var.bespoke_account}-imal-reporting-to-s3-raw"
  handler       = "lambda_function.lambda_handler"
  runtime       = "python3.12"
  timeout       = 900
  memory_size   = 2048
  layers        = [local.lambda_layer_aws_wrangler_arn]
  tracing_mode  = "Active"

  source_path = [{
    path = "${path.module}/../src/lambdas/imal_reporting_to_s3_raw/"
  }]

  environment_variables = {
    dest_bucket = local.raw_datalake_bucket_name
  }

  create_role              = false
  lambda_role              = aws_iam_role.iam_for_lambda.arn
  hash_extra               = "${local.prefix}-imal-reporting-to-s3-raw"
  recreate_missing_package = false
  ignore_source_code_hash  = true
}

###########################################################
# S3 -> EventBridge integration
###########################################################
resource "aws_s3_bucket_notification" "imal_reporting_eventbridge" {
  bucket      = data.aws_s3_bucket.s3_landing_bucket.id
  eventbridge = true
}

###########################################################
# EventBridge Rule: Filter bucket + key prefix (optional)
###########################################################
resource "aws_cloudwatch_event_rule" "imal_reporting_object_created" {
  name        = "imal-s3-obj-created-rule"
  description = "Trigger Lambda on S3 object creation under imal_reporting/"

  depends_on = [aws_s3_bucket_notification.imal_reporting_eventbridge]

  event_pattern = jsonencode({
    "source" : ["aws.s3"],
    "detail-type" : ["Object Created"],
    "detail" : {
      "bucket" : { "name" : [data.aws_s3_bucket.s3_landing_bucket.id] },
      "object" : { "key" : [{ "prefix" : "imal_reporting/" }] }
    }
  })
}

###########################################################
# EventBridge Target -> Lambda
###########################################################
resource "aws_cloudwatch_event_target" "imal_reporting_lambda_target" {
  rule      = aws_cloudwatch_event_rule.imal_reporting_object_created.name
  target_id = "lambda"
  arn       = module.lambda_imalreportingtos3.lambda_function_arn
}

###########################################################
# Lambda permission
###########################################################
resource "aws_lambda_permission" "imal_reporting_allow_eventbridge" {
  statement_id  = "AllowExecutionFromEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = module.lambda_imalreportingtos3.lambda_function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.imal_reporting_object_created.arn
}
