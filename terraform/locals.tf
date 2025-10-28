locals {
  project_name                   = "etl"
  prefix                         = "datalake-${local.project_name}"
  lambda_layer_aws_wrangler_arn  = "arn:aws:lambda:${var.region}:336392948345:layer:AWSSDKPandas-Python312:8"
  raw_datalake_bucket_name       = "bb2-${var.bespoke_account}-datalake-raw"
  curated_datalake_bucket_name   = "bb2-${var.bespoke_account}-datalake-curated"
  landing_datalake_bucket_name   = "bb2-${var.bespoke_account}-datalake-landing"
  athena_results_bucket_name     = "bb2-${var.bespoke_account}-datalake-athena-results"
  glue_assets_bucket_name        = "aws-glue-assets-${var.aws_account_id}-${var.region}"
  valuation_bucket_name          = "bb2-${var.bespoke_account}-datalake-document-extration-destination"
  amplitude_bucket_name          = "amplitude-export-129922-${var.bespoke_account}"
  glue_raw_database_name         = "datalake_raw"
  mortgage_valuations_table_name = "mortgage_valuations"
}
