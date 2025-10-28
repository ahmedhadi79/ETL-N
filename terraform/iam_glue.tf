resource "aws_iam_role" "iam_for_glue_etl" {
  name = "data_lake_iam_for_glue_etl"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = [
            "glue.amazonaws.com"
          ]
        }
      },
    ]
  })

  inline_policy {
    name   = "glue_iam_policy"
    policy = data.aws_iam_policy_document.glue_iam_policy_document.json
  }
}

################################################################################################
# Allfunds Glue IAM role
################################################################################################
resource "aws_iam_role" "iam_for_allfunds_glue_etl" {
  name = "data_lake_iam_for_allfunds_glue_etl"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = [
            "glue.amazonaws.com"
          ]
        }
      },
    ]
  })
}

resource "aws_iam_role_policy_attachment" "glue_role_policy" {
  role       = aws_iam_role.iam_for_allfunds_glue_etl.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy_attachment" "allfunds_glue_iam_policy_attachment" {
  role       = aws_iam_role.iam_for_allfunds_glue_etl.name
  policy_arn = aws_iam_policy.allfunds_glue_iam_policy.arn
}

resource "aws_iam_policy" "allfunds_glue_iam_policy" {
  name   = "allfunds-glue-iam-policy"
  policy = data.aws_iam_policy_document.allfunds_glue_iam_policy_document.json
}

data "aws_iam_policy_document" "allfunds_glue_iam_policy_document" {
  statement {
    actions = [
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:AbortMultipartUpload",
      "s3:ListBucket",
      "s3:GetBucketLocation"
    ]
    effect = "Allow"
    resources = [
      "arn:aws:s3:::${local.raw_datalake_bucket_name}",
      "arn:aws:s3:::${local.raw_datalake_bucket_name}/*",
      "arn:aws:s3:::${local.glue_assets_bucket_name}",
      "arn:aws:s3:::${local.glue_assets_bucket_name}/*",
      "arn:aws:s3:::${local.athena_results_bucket_name}",
      "arn:aws:s3:::${local.athena_results_bucket_name}/*",
    ]
  }

  statement {
    actions = [
      "secretsmanager:GetSecretValue"
    ]
    effect = "Allow"
    resources = [
      "arn:aws:secretsmanager:${var.region}:${var.aws_account_id}:secret:${var.allfunds_auth_path}-*",
    ]
  }
}

# AWS Lakeformation permissions
resource "aws_lakeformation_permissions" "glue_datalake_raw_database" {
  principal   = aws_iam_role.iam_for_allfunds_glue_etl.arn
  permissions = ["CREATE_TABLE"]

  database {
    name = "datalake_raw"
  }
}

resource "aws_lakeformation_permissions" "glue_datalake_raw_tables" {
  permissions = [
    "SELECT",
    "DESCRIBE",
    "INSERT",
    "DELETE",
    "INSERT",
    "ALTER",
    "DROP",
  ]

  principal = aws_iam_role.iam_for_allfunds_glue_etl.arn

  table {
    database_name = "datalake_raw"
    wildcard      = true
  }

  lifecycle {
    ignore_changes = [permissions]
  }
}

################################################################################################
# Clear Bank IAM role
################################################################################################
resource "aws_iam_role" "iam_for_clearbank_glue_etl" {
  name = "data_lake_iam_for_clearbank_glue_etl"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = [
            "glue.amazonaws.com"
          ]
        }
      },
    ]
  })

  inline_policy {
    name   = "clearbank_glue_iam_policy"
    policy = data.aws_iam_policy_document.clearbank_glue_iam_policy_document.json
  }
}

data "aws_iam_policy_document" "glue_iam_policy_document" {
  statement {
    actions = ["s3:*"]
    effect  = "Allow"
    resources = [
      "arn:aws:s3:::${local.raw_datalake_bucket_name}",
      "arn:aws:s3:::${local.raw_datalake_bucket_name}/*",
      "arn:aws:s3:::${local.glue_assets_bucket_name}",
      "arn:aws:s3:::${local.glue_assets_bucket_name}/*",
      "arn:aws:s3:::${local.landing_datalake_bucket_name}",
      "arn:aws:s3:::${local.landing_datalake_bucket_name}/*",
      "arn:aws:s3:::${local.athena_results_bucket_name}",
      "arn:aws:s3:::${local.athena_results_bucket_name}/*",
    ]
  }

  statement {
    effect = "Allow"
    actions = [
      "lakeformation:GetDataAccess",
    ]
    resources = ["*"]
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
    resources = ["arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:*"]
  }

  statement {
    actions = [
      "logs:DescribeLogGroups",
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents"
    ]
    effect    = "Allow"
    resources = ["*"]
  }
}

################################################################################################
# CLear Bank policy document
################################################################################################
data "aws_iam_policy_document" "clearbank_glue_iam_policy_document" {
  statement {
    actions = ["s3:*"]
    effect  = "Allow"
    resources = [
      "arn:aws:s3:::${local.raw_datalake_bucket_name}",
      "arn:aws:s3:::${local.raw_datalake_bucket_name}/*",
      "arn:aws:s3:::${local.glue_assets_bucket_name}",
      "arn:aws:s3:::${local.glue_assets_bucket_name}/*",
      "arn:aws:s3:::${local.athena_results_bucket_name}",
      "arn:aws:s3:::${local.athena_results_bucket_name}/*",
    ]
  }

  statement {
    effect = "Allow"
    actions = [
      "lakeformation:GetDataAccess",
    ]
    resources = ["*"]
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
    resources = ["arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:*"]
  }

  statement {
    actions = [
      "logs:DescribeLogGroups",
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents"
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
      "arn:aws:secretsmanager:${var.region}:${var.aws_account_id}:secret:${var.cb_auth_details}-*",
      "arn:aws:secretsmanager:${var.region}:${var.aws_account_id}:secret:${var.cb_api_key_details}-*",
    ]
  }

}

################################################################################################
# Lakeformation permissions
################################################################################################

# Datalake raw permissions
resource "aws_lakeformation_permissions" "database_lakeformation_permissions_glue_etl" {
  principal   = aws_iam_role.iam_for_glue_etl.arn
  permissions = ["CREATE_TABLE"]

  database {
    name = "datalake_raw"
  }

  lifecycle {
    ignore_changes = [permissions]
  }
}

resource "aws_lakeformation_permissions" "tables_lakeformation_permissions_glue_etl" {
  for_each    = toset(["datalake_raw"])
  permissions = ["SELECT", "DESCRIBE", "ALTER", "INSERT"]
  principal   = aws_iam_role.iam_for_glue_etl.arn

  table {
    database_name = each.key
    wildcard      = true
  }

  lifecycle {
    ignore_changes = [permissions]
  }
}
