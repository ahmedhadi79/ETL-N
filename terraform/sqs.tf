# resource "aws_sqs_queue" "queue_dlq" {
#   name                       = "imal_reporting_DLQ"
#   visibility_timeout_seconds = 1000
#   message_retention_seconds  = 1209600
# }

# resource "aws_sqs_queue" "queue" {
#   name                       = "imal_reporting_queue"
#   visibility_timeout_seconds = 1000
#   message_retention_seconds  = 1209600
#   delay_seconds              = var.bespoke_account == "prod" ? 30 : 15
#   redrive_policy = jsonencode({
#     deadLetterTargetArn = aws_sqs_queue.queue_dlq.arn
#     maxReceiveCount     = 3
#   })

#   # TODO: make it using data aws_iam_policy_document
#   policy = <<POLICY
# {
#   "Version": "2012-10-17",
#   "Statement": [
#     {
#       "Effect": "Allow",
#       "Principal": {
#         "Service": "s3.amazonaws.com"
#       },
#       "Action": "sqs:SendMessage",
#       "Resource": "arn:aws:sqs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:imal_reporting_queue",
#       "Condition": {
#         "ArnEquals": { "aws:SourceArn": "${data.aws_s3_bucket.landing_bucket_s3.arn}" }
#       }
#     }
#   ]
# }
# POLICY
# }
