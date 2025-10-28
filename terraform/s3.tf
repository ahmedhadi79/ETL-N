data "aws_s3_bucket" "sfmc" {
  bucket = var.sfmc_bucket
}

data "aws_s3_bucket" "sftp_paymentology_bucket" {
  bucket = var.s3_sftp_paymentology_bucket
}

data "aws_s3_bucket" "landing_bucket_s3" {
  bucket = local.landing_datalake_bucket_name
}

# resource "aws_s3_bucket_notification" "bucket_notification" {
#   bucket = data.aws_s3_bucket.landing_bucket_s3.id

#   # queue {
#   #   queue_arn     = aws_sqs_queue.queue.arn
#   #   events        = ["s3:ObjectCreated:*"]
#   #   filter_prefix = "imal_reporting/"
#   #   filter_suffix = ".json"
#   # }
# }
