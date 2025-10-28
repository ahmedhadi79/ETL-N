resource "aws_iam_role" "iam_for_lambda" {
  name = "data_lake_iam_for_lambda"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = [
            "lambda.amazonaws.com"
          ],
        }
      },
    ]
  })

  inline_policy {
    name   = "lambda_iam_policy"
    policy = data.aws_iam_policy_document.iam_policy_document.json
  }
}

data "aws_iam_policy_document" "iam_policy_document" {
  statement {
    actions = ["s3:*"]
    effect  = "Allow"
    resources = [
      "arn:aws:s3:::${local.raw_datalake_bucket_name}",
      "arn:aws:s3:::${local.raw_datalake_bucket_name}/*",
      "arn:aws:s3:::${local.curated_datalake_bucket_name}",
      "arn:aws:s3:::${local.curated_datalake_bucket_name}/*",
      "arn:aws:s3:::${local.landing_datalake_bucket_name}",
      "arn:aws:s3:::${local.landing_datalake_bucket_name}/*",
      "arn:aws:s3:::${var.s3_sftp_paymentology_bucket}",
      "arn:aws:s3:::${var.s3_sftp_paymentology_bucket}/*",
      "arn:aws:s3:::${var.s3_bucket_adjust_logs}",
      "arn:aws:s3:::${var.s3_bucket_adjust_logs}/*",
      "arn:aws:s3:::${var.sfmc_bucket}",
      "arn:aws:s3:::${var.sfmc_bucket}/*",
      "arn:aws:s3:::${local.glue_assets_bucket_name}",
      "arn:aws:s3:::${local.glue_assets_bucket_name}/*",
      "arn:aws:s3:::${local.amplitude_bucket_name}",
      "arn:aws:s3:::${local.amplitude_bucket_name}/*",
      "arn:aws:s3:::${local.athena_results_bucket_name}",
      "arn:aws:s3:::${local.athena_results_bucket_name}/*",
    ]
  }

  statement {
    actions   = ["athena:*"]
    effect    = "Allow"
    resources = ["*"]
  }

  statement {
    actions = [
      "glue:CreateDatabase",
      "glue:DeleteDatabase",
      "glue:GetDatabase",
      "glue:GetDatabases",
      "glue:UpdateDatabase",
      "glue:CreateTable",
      "glue:DeleteTable",
      "glue:BatchDeleteTable",
      "glue:UpdateTable",
      "glue:GetTable",
      "glue:GetTables",
      "glue:BatchCreatePartition",
      "glue:CreatePartition",
      "glue:DeletePartition",
      "glue:BatchDeletePartition",
      "glue:UpdatePartition",
      "glue:GetPartition",
      "glue:GetPartitions",
      "glue:BatchGetPartition"
    ]
    effect    = "Allow"
    resources = ["*"]
  }

  statement {
    actions = [
      "logs:DescribeLogGroups",
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents",
      "logs:FilterLogEvents",
      "logs:DescribeLogStream",
      "logs:GetLogEvents",
      "cloudwatch:GetMetricStatistics"
    ]
    effect    = "Allow"
    resources = ["*"]
  }

  statement {
    actions = [
      "secretsmanager:GetSecretValue"
    ]
    effect = "Allow"
    resources = [
      "arn:aws:secretsmanager:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:secret:${var.slscurrencycloud_auth_details}-*",
      "arn:aws:secretsmanager:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:secret:${var.url_currency_code}-*",
      "arn:aws:secretsmanager:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:secret:${var.cb_auth_details}-*",
      "arn:aws:secretsmanager:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:secret:${var.cb_api_key_details}-*",
      "arn:aws:secretsmanager:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:secret:sls/data/allFundsReadOnly-*",
    ]
  }

  statement {
    actions = [
      "ecr:*"
    ]
    effect    = "Allow"
    resources = ["*"]
  }

  statement {
    actions = [
      "xray:PutTraceSegments",
      "xray:PutTelemetryRecords",
      "xray:GetSamplingRules",
      "xray:GetSamplingTargets",
      "xray:GetSamplingStatisticSummaries"
    ]
    effect    = "Allow"
    resources = ["*"]
  }

  statement {
    sid = "AmplitudeSQSAccess"
    actions = [
      "sqs:ReceiveMessage",
      "sqs:DeleteMessage",
      "sqs:GetQueueAttributes"
    ]
    effect = "Allow"
    resources = [
      aws_sqs_queue.raw_amplitude_files_queue.arn
    ]
  }

  statement {
    sid = "PaymentologySQSAccess"
    actions = [
      "sqs:ReceiveMessage",
      "sqs:DeleteMessage",
      "sqs:GetQueueAttributes"
    ]
    effect = "Allow"
    resources = [
      aws_sqs_queue.raw_paymentology_files_queue.arn
    ]
  }

  statement {
    actions = [
      "ses:SendRawEmail",
      "ses:SendEmail"
    ]
    effect    = "Allow"
    resources = ["arn:aws:ses:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:identity/${var.bespoke_account}.bb2bank.io"]
  }
}

################################################################################################
# Lakeformation permissions
################################################################################################

# Datalake raw permissions
resource "aws_lakeformation_permissions" "lambda_datalake_raw_database" {
  principal   = aws_iam_role.iam_for_lambda.arn
  permissions = ["CREATE_TABLE"]

  database {
    name = "datalake_raw"
  }

  lifecycle {
    ignore_changes = [permissions]
  }
}

resource "aws_lakeformation_permissions" "lambda_datalake_raw_tables" {
  permissions = [
    "SELECT",
    "DESCRIBE",
    "INSERT",
    "DELETE",
    "INSERT",
    "ALTER",
  ]

  principal = aws_iam_role.iam_for_lambda.arn

  table {
    database_name = "datalake_raw"
    wildcard      = true
  }

  lifecycle {
    ignore_changes = [permissions]
  }
}

# Datalake Curated permissions
resource "aws_lakeformation_permissions" "lambda_datalake_curated_database" {
  principal   = aws_iam_role.iam_for_lambda.arn
  permissions = ["CREATE_TABLE"]

  database {
    name = "datalake_curated"
  }
  
  lifecycle {
    ignore_changes = [permissions]
  }
}

resource "aws_lakeformation_permissions" "lambda_datalake_curated_tables" {
  permissions = [
    "SELECT",
    "DESCRIBE",
    "INSERT",
    "DELETE",
    "INSERT",
    "ALTER",
  ]

  principal = aws_iam_role.iam_for_lambda.arn

  table {
    database_name = "datalake_curated"
    wildcard      = true
  }

  lifecycle {
    ignore_changes = [permissions]
  }
}
