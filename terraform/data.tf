data "aws_region" "current" {}
data "aws_caller_identity" "current" {}

data "aws_ssm_parameter" "s3_access_logs_bucket" {
  name = "/bb2bank/s3/access_logs_bucket"
}

data "aws_s3_bucket" "s3_landing_bucket" {
  bucket = "bb2-${var.bespoke_account}-datalake-landing"
}

################################################################################################
# VPC data
################################################################################################

data "aws_ssm_parameter" "sls_vpc_private_subnet_ids" {
  name = "/nomo/nomo_networking/sls_vpc/vpc/private_subnets/ids"
}

data "aws_subnet" "selected_private_subnet" {
  id = split(",", data.aws_ssm_parameter.sls_vpc_private_subnet_ids.value)[0]
}

data "aws_ssm_parameter" "sls_vpc_security_group_allow_tls_id" {
  name = "/nomo/nomo_networking/sls_vpc/security_group/allow_tls/id"
}
data "aws_ssm_parameter" "sls_vpc_security_group_allow_s3_gw_id" {
  name = "/nomo/nomo_networking/sls_vpc/security_group/allow_s3_gw/id"
}

data "aws_security_group" "allow_glue_sg" {
  name = "allow_glue"
}
