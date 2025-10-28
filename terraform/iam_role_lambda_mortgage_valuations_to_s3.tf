resource "aws_iam_role" "valuation_lambda_role" {
  name = "valuation_lambda_role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      }
    ]
  })
  inline_policy {
    name   = "lambda_iam_policy"
    policy = data.aws_iam_policy_document.iam_valuation_policy_document.json
  }
}

data "aws_iam_policy_document" "iam_valuation_policy_document" {
  statement {
    actions = [
      "s3:GetObject",
      "s3:ListBucket",
      "s3:PutObject",
      "s3:GetBucketLocation"
    ]

    effect = "Allow"
    resources = [
      "arn:aws:s3:::${local.raw_datalake_bucket_name}",
      "arn:aws:s3:::${local.raw_datalake_bucket_name}/*",
      "arn:aws:s3:::${local.valuation_bucket_name}",
      "arn:aws:s3:::${local.valuation_bucket_name}/*",
      "arn:aws:s3:::${local.athena_results_bucket_name}",
      "arn:aws:s3:::${local.athena_results_bucket_name}/*",
    ]
  }

  statement {
    actions = [
      "s3:DeleteObject",
    ]

    effect = "Allow"
    resources = [
      "arn:aws:s3:::${local.raw_datalake_bucket_name}/${local.mortgage_valuations_table_name}/*",
    ]
  }

  statement {
    actions = [
      "glue:GetDatabase",
      "glue:GetTable",
      "glue:CreateTable",
      "glue:UpdateTable",
      "glue:CreatePartition",
      "glue:BatchCreatePartition",
      "glue:GetPartition",
      "glue:BatchGetPartition",
      "glue:GetPartitions",
      "glue:CreatePartition",
      "glue:BatchCreatePartition",
      "glue:DeletePartition",
      "glue:BatchDeletePartition",
      "glue:UpdatePartition",
      "glue:BatchUpdatePartition",
    ]
    effect = "Allow"
    resources = [
      "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:catalog",
      "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:database/${local.glue_raw_database_name}",
      "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:table/${local.glue_raw_database_name}/${local.mortgage_valuations_table_name}"
    ]
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

}

resource "aws_lakeformation_permissions" "lambda_datalake_raw_database_valuation" {
  principal   = aws_iam_role.valuation_lambda_role.arn
  permissions = ["CREATE_TABLE"]

  database {
    name = local.glue_raw_database_name
  }

  lifecycle {
    ignore_changes = [permissions]
  }
}

resource "aws_lakeformation_permissions" "lambda_datalake_valuation_tables" {
  permissions = [
    "SELECT",
    "DESCRIBE",
    "INSERT",
    "DELETE",
    "ALTER",
  ]

  principal = aws_iam_role.valuation_lambda_role.arn

  table {
    database_name = local.glue_raw_database_name
    wildcard      = true
  }

  lifecycle {
    ignore_changes = [permissions]
  }
}

