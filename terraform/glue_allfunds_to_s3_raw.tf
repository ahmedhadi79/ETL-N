######################################################################
# AWS Glue pyshell function: allfunds to s3 raw
######################################################################
resource "aws_s3_object" "glue_allfunds_files" {
  for_each = fileset("../src/glue/allfunds_to_s3_raw/", "*.py")

  bucket = local.glue_assets_bucket_name
  key    = "${local.project_name}/scripts/allfunds_to_s3_raw/${each.value}"
  source = "../src/glue/allfunds_to_s3_raw/${each.value}"
  etag   = filemd5("../src/glue/allfunds_to_s3_raw/${each.value}")
}


resource "aws_glue_job" "glue_allfunds_to_s3_raw" {
  name        = "${local.prefix}-allfunds-to-s3-raw"
  description = "AWS Glue Pyshell Job Allfunds ETL"

  role_arn     = aws_iam_role.iam_for_allfunds_glue_etl.arn
  max_retries  = 0
  max_capacity = 1
  timeout      = 480
  command {
    name            = "pythonshell"
    script_location = "s3://${local.glue_assets_bucket_name}/${local.project_name}/scripts/allfunds_to_s3_raw/glue_function.py"
    python_version  = 3.9
  }

  default_arguments = {
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                   = "true"
    "--TempDir"                          = "s3://${local.glue_assets_bucket_name}/temporary/"
    "--enable-glue-datacatalog"          = "true"
    "--extra-py-files" = join(",",
      [
        for file in fileset("../src/glue/allfunds_to_s3_raw/", "*.py") :
        "s3://${local.glue_assets_bucket_name}/${local.project_name}/scripts/allfunds_to_s3_raw/${file}"
      ]
    )

    "--ATHENA_TABLE_NAMES"      = "['allfunds_transactions_open_positions','allfunds_transactions_performance']",
    "--S3_RAW"                  = local.raw_datalake_bucket_name,
    "--NOMO_ALLFUNDS_READ_ONLY" = var.allfunds_auth_path,
    "--BASE_URL"                = var.allfunds_base_url,
  }

  connections = ["${local.prefix}-vpc-private-connection"]

}

resource "aws_glue_connection" "glue_allfunds_vpc_private_connection" {
  name            = "${local.prefix}-vpc-private-connection"
  connection_type = "NETWORK"

  physical_connection_requirements {
    subnet_id         = data.aws_subnet.selected_private_subnet.id
    availability_zone = data.aws_subnet.selected_private_subnet.availability_zone
    security_group_id_list = [
      data.aws_ssm_parameter.sls_vpc_security_group_allow_tls_id.value,   #For secrets manager
      data.aws_ssm_parameter.sls_vpc_security_group_allow_s3_gw_id.value, #For S3 scripts download, athena, etc
      data.aws_security_group.allow_glue_sg.id                            #For Glue job to communicate with itself
    ]
  }
}

resource "aws_glue_trigger" "glue_allfunds_to_s3_raw_trigger" {
  name     = "glue_allfunds_to_s3_raw_trigger"
  schedule = "cron(00 07 * * ? *)"
  type     = "SCHEDULED"
  enabled  = "true"

  actions {
    job_name = aws_glue_job.glue_allfunds_to_s3_raw.name
  }
}
