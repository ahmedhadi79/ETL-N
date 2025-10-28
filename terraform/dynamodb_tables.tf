# DynamoDB
resource "aws_dynamodb_table" "jira_tickets_table" {
  name         = "jira_tickets"
  billing_mode = "PAY_PER_REQUEST" #"PROVISIONED"
  hash_key     = "key"

  deletion_protection_enabled = true

  point_in_time_recovery {
    enabled = true
  }


  attribute {
    name = "key"
    type = "S"
  }
}
