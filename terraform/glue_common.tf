resource "aws_s3_object" "glue_custom_functions" {
  bucket = local.glue_assets_bucket_name
  key    = "${local.project_name}/scripts/common/custom_functions.py"
  source = "../src/common/custom_functions.py"

  etag = filemd5("../src/common/custom_functions.py")
}

resource "aws_s3_object" "glue_api_client" {
  bucket = local.glue_assets_bucket_name
  key    = "${local.project_name}/scripts/common/api_client.py"
  source = "../src/common/api_client.py"

  etag = filemd5("../src/common/api_client.py")
}

