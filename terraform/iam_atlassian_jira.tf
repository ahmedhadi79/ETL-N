resource "aws_iam_role" "iam_for_atlassian_jira" {
  name = "data_lake_iam_for_atlassian_jira"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = [
            "lambda.amazonaws.com"
          ]
        }
      },
    ]
  })
  inline_policy {
    name   = "atlassian_jira_inline_policy"
    policy = data.aws_iam_policy_document.iam_policy_document_atlassian_jira.json
  }
}


################################################################################################

#### Use the below policy document for new data sources above document is full...
data "aws_iam_policy_document" "iam_policy_document_atlassian_jira" {
  statement {
    actions = ["s3:*"]
    effect  = "Allow"
    resources = [
      "arn:aws:s3:::${local.raw_datalake_bucket_name}",
      "arn:aws:s3:::${local.raw_datalake_bucket_name}/*",
    ]
  }

  statement {
    actions   = ["athena:*"]
    effect    = "Allow"
    resources = ["*"]
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
    sid = "ReadWriteTable"
    actions = [
      "dynamodb:BatchGetItem",
      "dynamodb:GetItem",
      "dynamodb:Query",
      "dynamodb:Scan",
      "dynamodb:DescribeTable",
      "dynamodb:PutItem",
      "dynamodb:UpdateItem"
    ]
    effect = "Allow"
    resources = [
      aws_dynamodb_table.jira_tickets_table.arn
    ]
  }

  statement {
    actions = [
      "glue:CreateTable",
      "glue:GetTable",
      "glue:GetTables",
      "glue:UpdateTable",
      "glue:DeleteTable",
      "glue:BatchCreatePartition",
      "glue:GetDatabase",
      "glue:GetDatabases",
      "glue:SearchTables",
      "glue:GetTableVersion",
      "glue:GetTableVersions",
      "glue:GetPartitions"
    ]
    effect    = "Allow"
    resources = ["*"]
  }

  statement {
    effect = "Allow"
    actions = [
      "lakeformation:GetDataAccess",
      "lakeformation:GrantPermissions",
      "lakeformation:GetDataLakeSettings",
      "lakeformation:PutDataLakeSettings",
      "lakeformation:RevokePermissions",
      "lakeformation:BatchGrantPermissions",
      "lakeformation:BatchRevokePermissions",
      "lakeformation:ListPermissions"
    ]
    resources = ["*"]
  }
}

################################################################################################
# Lakeformation permissions
################################################################################################

# Datalake raw permissions
resource "aws_lakeformation_permissions" "lakeformation_permissions_jira_database_resources" {
  principal   = aws_iam_role.iam_for_atlassian_jira.arn
  permissions = ["CREATE_TABLE"]

  database {
    name = "datalake_raw"
  }

  lifecycle {
    ignore_changes = [permissions]
  }
}

resource "aws_lakeformation_permissions" "lakeformation_permissions_jira_tickets" {
  for_each = toset(["datalake_raw"])

  permissions = ["SELECT", "DESCRIBE", "ALTER", "ALL"]
  principal   = aws_iam_role.iam_for_atlassian_jira.arn

  table {
    database_name = each.key
    wildcard      = true
  }

  lifecycle {
    ignore_changes = [permissions]
  }
}
