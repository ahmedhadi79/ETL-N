###########################################################
# AWS Glue Job: sfmc partnership incentive reporting
###########################################################
resource "aws_s3_object" "util_glue" {
  bucket = local.glue_assets_bucket_name
  key    = "${local.project_name}/scripts/util_glue.py"
  source = "../src/glue/util_glue/util_glue.py"

  etag = filemd5("../src/glue/util_glue/util_glue.py")
}

resource "aws_cloudwatch_log_group" "util_glue" {
  name              = "/aws-glue/jobs/datalake-${var.bespoke_account}-util-glue"
  retention_in_days = 14
}


resource "aws_glue_job" "util_glue" {
  name              = "${local.prefix}-util-glue"
  description       = "AWS Glue Job util glue function"
  role_arn          = aws_iam_role.iam_for_clearbank_glue_etl.arn
  glue_version      = "4.0"
  number_of_workers = 10
  worker_type       = "G.1X"
  max_retries       = "0"
  timeout           = 120
  command {
    name            = "glueetl"
    script_location = "s3://${local.glue_assets_bucket_name}/${aws_s3_object.util_glue.key}"
    python_version  = 3
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--TempDir"                          = "s3://${local.glue_assets_bucket_name}/temporary/"
    "--S3_RAW"                           = local.raw_datalake_bucket_name
    "--enable-auto-scaling"              = "true"
     "--continuous-log-logGroup"          = aws_cloudwatch_log_group.util_glue.name
    "--cloudwatch-log-stream-prefix"     = "util_glue"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                   = "true"
    "--enable-spark-ui"                  = "true"
    "--enable-glue-datacatalog"          = "true"
    "--spark-event-logs-path"            = "s3://${local.glue_assets_bucket_name}/sparkHistoryLogs/"
    "--TempDir"                          = "s3://${local.glue_assets_bucket_name}/temporary/"
    "--ENV"                              = var.bespoke_account

  }
}
