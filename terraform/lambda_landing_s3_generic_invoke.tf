# module "lambda_landing_s3_generic_invoke" {
#   source  = "terraform-aws-modules/lambda/aws"
#   version = "7.9.0"

#   function_name = "${local.prefix}-s3-landing-generic-invoke"
#   handler       = "lambda_function.lambda_handler"
#   runtime       = "python3.12"
#   timeout       = 900
#   memory_size   = 10240
#   layers        = [local.lambda_layer_aws_wrangler_arn]

#   source_path = [
#     {
#       path             = "${path.module}/../src/lambdas/landing_s3_generic_invoke",
#       pip_requirements = true,
#       patterns = [
#         "!tests/.*",
#         "!backfill/.*",
#         "!.DS_Store",
#         "!.*.md",
#         "!.package/.*",
#       ]
#     },
#   ]

#   environment_variables = {
#     DATALAKE_LANDING_S3BUCKET = local.landing_datalake_bucket_name,
#     IMAL_S3_KEY               = "imal_reporting",
#     GLUE_NAME_IMAL_REPORT     = aws_glue_job.datalake_imal_reporting_to_s3_raw.name,
#   }

#   hash_extra   = "${local.prefix}-s3-landing-generic-invoke"
#   create_role  = false
#   lambda_role  = aws_iam_role.iam_for_lambda.arn
#   tracing_mode = "Active"

#   recreate_missing_package = false
#   ignore_source_code_hash  = true
# }

# # Event source from SQS
# resource "aws_lambda_event_source_mapping" "lambda_landing_s3_generic_invoke_event_mapping" {
#   event_source_arn                   = aws_sqs_queue.queue.arn
#   enabled                            = false
#   function_name                      = module.lambda_landing_s3_generic_invoke.lambda_function_arn
#   batch_size                         = 1
#   maximum_batching_window_in_seconds = 2
#   scaling_config {
#     maximum_concurrency = 10
#   }
# }
