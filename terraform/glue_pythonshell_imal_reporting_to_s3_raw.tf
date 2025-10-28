resource "aws_s3_object" "datalake_imal_reporting_to_s3_raw" {
  bucket = local.glue_assets_bucket_name
  key    = "${local.project_name}/scripts/imal_reporting_to_s3_raw.py"
  source = "../src/glue/imal_reporting_to_s3_raw/imal_reporting_to_s3_raw.py"

  etag = filemd5("../src/glue/imal_reporting_to_s3_raw/imal_reporting_to_s3_raw.py")
}

resource "aws_glue_job" "datalake_imal_reporting_to_s3_raw" {
  name        = "${local.prefix}-imal-reporting-to-s3-raw"
  role_arn    = aws_iam_role.iam_for_glue_etl.arn
  max_retries = "0"
  timeout     = 120
  command {
    name            = "pythonshell"
    script_location = "s3://${local.glue_assets_bucket_name}/${aws_s3_object.datalake_imal_reporting_to_s3_raw.key}"
    python_version  = 3.9
  }
  default_arguments = {
    "--enable-auto-scaling"              = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                   = "true"
    "--TempDir"                          = "s3://${local.glue_assets_bucket_name}/temporary/"
    "--enable-glue-datacatalog"          = "true"
    "--S3_RAW"                           = local.raw_datalake_bucket_name
    "--bucket_name"                      = local.landing_datalake_bucket_name
    "--backfill"                         = "false"
    "--start_date"                       = "2020-01-01"
    "--end_date"                         = "2020-01-01"
    "--valid_files"                      = "CardTransactions,CurrentAccountBalanceByDay,FixedTermDepositFeesAndProfit,CurrentAccountFTPandCompensation,FixedTermDepositsByDay,MortgageBalanceByDay,MortgageFeesAndExpenses,MortgageProfit,FixedTermDepositFTP,Cashback,CardChargesAndFees,CurrentAccountFeesAndProfit,MultiCurrencyFeesAndIncome,ArrearsDays,OvernightRates"
    "library-set"                        = "analytics"
    "--additional-python-modules"        = "ijson==3.3.0"
  }
  max_capacity = 1
}

###########################################################
# AWS Glue Triggers - datalake_imal_reporting_to_s3_raw
###########################################################
resource "aws_glue_trigger" "datalake_imal_reporting_to_s3_raw_trigger" {
  name     = "${local.prefix}-imal-reporting-to-s3-raw_trigger"
  schedule = "cron(00 09 * * ? *)"
  type     = "SCHEDULED"
  enabled  = var.datalake_imal_reporting_to_s3_raw_enabled

  actions {
    job_name = aws_glue_job.datalake_imal_reporting_to_s3_raw.name
  }
}
