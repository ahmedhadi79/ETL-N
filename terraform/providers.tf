provider "aws" {
  region              = "eu-west-2"
  allowed_account_ids = [var.aws_account_id]
  assume_role {
    role_arn    = "arn:aws:iam::${var.aws_account_id}:role/${var.resource_management_iam_role}"
    external_id = var.external_id
  }
  default_tags {
    tags = {
      Project      = var.project_url
      Region       = "eu-west-2"
      ManagedBy    = "Terraform"
      map-migrated = "d-server-02qbxzpy9ejx1d"
      service      = "data-lake-etl"
      env          = var.bespoke_account
      Team         = "Data"
    }
  }
}
