##############################################
# Lambda: cards_paymentology_data_to_s3_raw
##############################################
module "lambda_cards_paymentology_data_to_s3" {
  source  = "terraform-aws-modules/lambda/aws"
  version = "7.9.0"

  function_name = "${local.prefix}-cards-paymentology-data-to-s3-raw"
  handler       = "lambda_function.lambda_handler"
  runtime       = "python3.12"
  timeout       = 900
  memory_size   = 2048
  layers        = [local.lambda_layer_aws_wrangler_arn]

  source_path = [
    "${path.module}/../src/lambdas/cards_paymentology_data_to_s3_raw",
  ]

  environment_variables = {
    S3_RAW = local.raw_datalake_bucket_name
  }

  create_role              = false
  lambda_role              = aws_iam_role.iam_for_lambda.arn
  tracing_mode             = "Active"
  hash_extra               = "${local.prefix}-cards-paymentology-data-to-s3-raw"
  recreate_missing_package = false
  ignore_source_code_hash  = true
}
