###########################################################
# AWS Lambda function: MIR amplitude sqs
###########################################################
module "lambda_mir_amplitude" {

  source  = "terraform-aws-modules/lambda/aws"
  version = "7.9.0"

  function_name = "datalake-${var.bespoke_account}-mir-amplitude"
  handler       = "lambda_function.lambda_handler"
  runtime       = "python3.12"
  timeout       = 900
  layers        = [local.lambda_layer_aws_wrangler_arn]
  memory_size   = 2048

  source_path = [
    {
      path             = "${path.module}/../src/lambdas/mir_amplitude/",
    }
  ]

  environment_variables = {
    dest_bucket    = local.curated_datalake_bucket_name,
  }

  hash_extra   = "${local.prefix}-mir-amplitude"
  create_role  = false
  lambda_role  = aws_iam_role.iam_for_lambda.arn
  tracing_mode = "Active"

  recreate_missing_package = false
  ignore_source_code_hash  = true
}