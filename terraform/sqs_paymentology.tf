##############################################
# S3 -> SQS -> Lambda wiring for Paymentology
##############################################

# Source S3 bucket hosting incoming Paymentology files
data "aws_s3_bucket" "s3_bucket_raw_paymentology" {
  bucket = var.s3_sftp_paymentology_bucket
}

##############################################
# SQS Queues (DLQ + main)
##############################################

resource "aws_sqs_queue" "raw_paymentology_files_dlq" {
  name                    = "${local.prefix}-raw-paymentology-files-dlq"
  sqs_managed_sse_enabled = true
}

resource "aws_sqs_queue" "raw_paymentology_files_queue" {
  name = "${local.prefix}-raw-paymentology-files-queue"

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.raw_paymentology_files_dlq.arn
    maxReceiveCount     = 2
  })

  # keep >= Lambda timeout
  visibility_timeout_seconds = 900
  sqs_managed_sse_enabled    = true
}

resource "aws_sqs_queue_policy" "raw_paymentology_files_queue_policy" {
  queue_url = aws_sqs_queue.raw_paymentology_files_queue.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "AllowS3SendMessage"
        Effect    = "Allow"
        Principal = { Service = "s3.amazonaws.com" }
        Action    = "sqs:SendMessage"
        Resource  = aws_sqs_queue.raw_paymentology_files_queue.arn
        Condition = {
          ArnEquals = {
            "aws:SourceArn" = data.aws_s3_bucket.s3_bucket_raw_paymentology.arn
          }
        }
      }
    ]
  })
}

data "aws_iam_policy_document" "queue_s3_access_paymentology" {
  statement {
    effect = "Allow"

    principals {
      type        = "*"
      identifiers = ["*"]
    }

    actions   = ["sqs:SendMessage"]
    resources = [aws_sqs_queue.raw_paymentology_files_queue.arn]

    condition {
      test     = "ArnEquals"
      variable = "aws:SourceArn"
      values   = [data.aws_s3_bucket.s3_bucket_raw_paymentology.arn]
    }
  }
}
##############################################
# S3 -> SQS Event Notification
##############################################

resource "aws_s3_bucket_notification" "raw_bucket_notification_paymentology" {
  bucket = data.aws_s3_bucket.s3_bucket_raw_paymentology.id

  queue {
    queue_arn     = aws_sqs_queue.raw_paymentology_files_queue.arn
    events        = ["s3:ObjectCreated:*"]
  }
}

##############################################
# SQS -> Lambda trigger
##############################################

resource "aws_lambda_event_source_mapping" "lambda_raw_to_paymentology_trigger" {
  count            = contains(["prod", "beta", "alpha", "sandbox"], var.bespoke_account) ? 1 : 0
  event_source_arn = aws_sqs_queue.raw_paymentology_files_queue.arn
  function_name    = module.lambda_cards_paymentology_data_to_s3.lambda_function_arn

  scaling_config {
    maximum_concurrency = 100
  }

  batch_size                         = 50
  maximum_batching_window_in_seconds = 30
  enabled                            = true
}