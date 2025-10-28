data "aws_s3_bucket" "s3_bucket_raw_amplitude" {
  bucket = "amplitude-export-129922-${var.bespoke_account}"
}

resource "aws_sqs_queue" "raw_amplitude_files_dlq" {
  name                    = "datalake-raw-amplitude-files-dlq"
  sqs_managed_sse_enabled = true
}

resource "aws_sqs_queue" "raw_amplitude_files_queue" {
  name = "datalake-raw-amplitude-files-queue"

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.raw_amplitude_files_dlq.arn
    maxReceiveCount     = 10
  })

  policy = <<POLICY
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "s3.amazonaws.com"
      },
      "Action": "sqs:SendMessage",
      "Resource": "arn:aws:sqs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:datalake-raw-amplitude-files-queue",
      "Condition": {
        "ArnEquals": {
          "aws:SourceArn": "${data.aws_s3_bucket.s3_bucket_raw_amplitude.arn}"
        }
      }
    }
  ]
}
POLICY

  visibility_timeout_seconds = 900
  sqs_managed_sse_enabled    = true
}

# ✅ Unique IAM policy document name
data "aws_iam_policy_document" "queue_s3_access_amplitude" {
  statement {
    effect = "Allow"

    principals {
      type        = "*"
      identifiers = ["*"]
    }

    actions   = ["sqs:SendMessage"]
    resources = [aws_sqs_queue.raw_amplitude_files_queue.arn]

    condition {
      test     = "ArnEquals"
      variable = "aws:SourceArn"
      values   = [data.aws_s3_bucket.s3_bucket_raw_amplitude.arn]
    }
  }
}

# ✅ S3 event notification for amplitude table only
resource "aws_s3_bucket_notification" "raw_bucket_notification_amplitude" {
  bucket = data.aws_s3_bucket.s3_bucket_raw_amplitude.id

  queue {
    queue_arn     = aws_sqs_queue.raw_amplitude_files_queue.arn
    events        = ["s3:ObjectCreated:*"]

    filter_prefix = "${var.amplitude_table_name}/"
    filter_suffix = ".json.gz"
  }
}

# ✅ Lambda trigger
resource "aws_lambda_event_source_mapping" "lambda_raw_to_amplitude_trigger" {
  count            = contains(["prod", "beta", "alpha", "sandbox"], var.bespoke_account) ? 1 : 0
  event_source_arn = aws_sqs_queue.raw_amplitude_files_queue.arn
  function_name    = module.lambda_mir_amplitude.lambda_function_arn

  scaling_config {
    maximum_concurrency = 100
  }

  batch_size                         = 50
  maximum_batching_window_in_seconds = 30
  enabled                            = true
}
