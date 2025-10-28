# Data Lake Infrastructure as Code Terraform

Terraform, Lambda functions and layers for the data lake project.

## Getting started

### Pre-Commit
1. Install pre-commit from https://pre-commit.com/#install
2. Install the Gitleaks Pre-Commit Hook
```bash
pre-commit install
```
3. Test the Installation
```bash
pre-commit run --all-files
```
4. Committing Changes

    Now, every time your team members attempt to commit changes, the pre-commit hook will automatically run Gitleaks to scan for secrets.



# Checklist for technical implementation 

## A. Resources to be defined in this repo

For each Lambda:

- Develop Lambda logic with Python 3.7
    - Assess if the dataset will be written in deltas or full overwrite
    - Assess if the dataset will be written in tabular format or if it is intended for high performance columnar analytics then parquet
    - Lambda should write to datalake_raw database. See [here](https://gitlab.com/bb2-bank/infrastructure/terraform-live/data-lake-etl/-/blob/main/lambda/dynamocustomerstos3raw/lambda_function.py#L175)for an example.
    - A schema should be supplied in the write function with correct data type columns for each column
    - Assess if partitioning is required at this stage, a good idea is to at least partition by date
- If new Lambda layers are required integrate with the Lambda function. Define the steps for building the layer and pushing in S3 in this repo
- Develop appropriate unit tests like in [here](https://gitlab.com/bb2-bank/infrastructure/terraform-live/data-lake-etl/-/blob/main/lambda/dynamocustomerstos3raw/tests/test_get_athena_schema.py)
    - Tests should also be part of the CI pipeline, like in [here](https://gitlab.com/bb2-bank/infrastructure/terraform-live/data-lake-etl/-/blob/main/.gitlab-ci.yml#L136)
    - TODO: A new bash script will be written to execute from the CI. The script contents will execute the tests with `pytest`

## B. Testing

Use `sandbox` or `alpha` to deploy. For example if the Lamdba is `quicksight-authors-datasets`:

- `aws configure sso --profile bb2-alpha-admin`
    - `SSO start URL [https://bb2.awsapps.com/start#/]`
- [aws lambda update-function-code](https://awscli.amazonaws.com/v2/documentation/api/latest/reference/lambda/update-function-code.html).
```bash
cd ./functions/quicksight-authors-datasets 
zip quicksight-authors-datasets.zip lambda_function.py 6185_customer_mambu.sql 14108_customer_detail.sql config.py 
aws lambda update-function-code \
    --function-name  datalake-alpha-quicksight-authors-datasets-to-s3-curated \
    --zip-file fileb://quicksight-authors-datasets.zip \
    --profile bb2-alpha-admin && rm quicksight-authors-datasets.zip
```

- [aws lambda publish-layer-version](https://awscli.amazonaws.com/v2/documentation/api/latest/reference/lambda/publish-layer-version.html)

And to test:
- [aws lambda invoke](https://awscli.amazonaws.com/v2/documentation/api/latest/reference/lambda/invoke.html)
```bash
aws lambda invoke \
    --cli-binary-format raw-in-base64-out \
    --function-name datalake-alpha-quicksight-authors-datasets-to-s3-curated \
    --profile bb2-alpha-admin \
    response.json
```

Inspect AWS console and `response.json` for results of invoking the function. Then `rm response.json`.

## C. Resources to be defined in Terraform
- Lambdas/Layers are deployed via Terraform via https://gitlab.com/bb2-bank/infrastructure/terraform-live/data-lake-etl/-/tree/main/terraform
- Cloudwatch log group for Lambda
    - Create a new log group and define with Lambda
    - Retention of logs must be 2 weeks
- If source is a Dynamo table, use Kinesis Data Stream as event source mapping. If ingestion happens in schedule use Cloudwatch EventBridge for scheduling if no real time option is available
- Cloudwatch monitoring and alarms should exist like in https://gitlab.com/bb2-bank/infrastructure/terraform-live/data-lake-etl/-/blob/main/terraform/cloudwatch_monitoring.tf
- Cloudwatch log aggregation should be configured for each lambda function, for this following module should be used: https://gitlab.com/bb2-bank/infrastructure/terraform-modules/terraform-aws-cloudwatch-aggregation

An example `terraform` deployment for a Lambda function with DynamoDB table as input:
```terraform
module "lambda_dynamoslscardstos3" {
 source = "terraform-aws-modules/lambda/aws"
 version = "2.27.1"

 function_name = var.lambda_dynamoslscardstos3_name
 handler       = "lambda_function.lambda_handler"
 runtime       = "python3.7"
 timeout       = "600"
 layers        = [aws_lambda_layer_version.aws_wrangler.arn, aws_lambda_layer_version.flattenjson.arn]

 source_path = [
  "${path.module}/../lambda/dynamoslscardstos3raw/lambda_function.py",
  "${path.module}/../lambda/dynamoslscardstos3raw/config.py"
 ]

 environment_variables = var.lambda_dynamo_cards_env_vars
 recreate_missing_package = false
 ignore_source_code_hash = true
 use_existing_cloudwatch_log_group = true
 create_role = false
 lambda_role = aws_iam_role.iam_for_lambda.arn
}
```

An example `terraform` deployment for a Lambda function with Cloudwatch schedule:

```terraform
module "lambda_salesforcecasestos3" {

  source = "terraform-aws-modules/lambda/aws"
  version = "2.28.0"

  function_name = var.lambda_salesforcecasestos3_name
  handler       = "lambda_function.lambda_handler"
  runtime       = "python3.7"
  timeout       = "600"
  layers        = [aws_lambda_layer_version.aws_wrangler.arn]

  source_path = "${path.module}/../lambda/salesforcecasestos3raw"

  environment_variables = var.lambda_salesforcecases_env_vars
  recreate_missing_package = false
  ignore_source_code_hash = true
  use_existing_cloudwatch_log_group = true
  create_role = false
  lambda_role = aws_iam_role.iam_for_lambda.arn

  depends_on = [
    aws_cloudwatch_log_group.lambda_salesforcecasestos3_log_group,
  ]
}


```

An example `terraform` deployment for a Lambda function with Cloudwatch log aggregation:
In this case `aws_cloudwatch_log_group` created for `lambda_salesforcecasestos3` lambda function.

```terraform
resource "aws_cloudwatch_log_group" "lambda_salesforcecasestos3_log_group" {
  name              = "/aws/lambda/${var.lambda_salesforcecasestos3_name}"
  retention_in_days = 14
}

```

- In addition, build the reconciliation logic and a new Quicksight dashboard(added as a separate tab in the existing dashboard). The reconciliation logic for Dynamo tables is found in https://gitlab.com/bb2-bank/infrastructure/terraform-live/data-lake-etl/blob/fef5ab6a29313adf4638043195cec8962c084b3a/lambda/dynamodb-datalake-reconciliation/lambda_function.py , otherwise if the source is not Dynamo, there is a separate Lambda for Mambu and Salesforce.

# Lambda Functions with Special Deployment strategy

There are a few Lambda functions that require special installation due to the libraries that are being used. Because when we deploy each Lambda the deployment takes place in the same container(as part of the CI/CD), when a MR contains changes to two or more of those “special” functions, it will fail since Terraform will try to execute the deployment in parallel. And the commands for those “special” functions can break each other’s deployment. The Lambda special functions are:
- mambu-datalake-reconciliation
- mambu-to-s3-raw
- dynamodb-datalake-reconciliation

## What do I need to be careful with?
So in essence, please when making changes in any of these lambdas, please do them 1 to 1 per each MR. For example 1 MR that contains changes in 2 or more is not guaranteed to succeed and should be avoided.

## What makes them special? 

If we see the Terraform component which deploys the Lambda(https://gitlab.com/bb2-bank/infrastructure/terraform-live/data-lake-etl/-/blob/main/terraform/lambda_mambu.tf) and particularly this piece of code:
```
  source_path = [
    {
      path = "${path.module}/../lambda/mambu-to-s3-raw",
      commands = [
        "python3 -m pip install pip==19.2",
        "cd ${abspath(path.module)}/../lambda/mambu-to-s3-raw",
        "mkdir package",
        "python3 -m pip install --target package -r requirements.txt",
        "mv package/bin/tap-mambu package/",
        "mv package/bin/target-jsonl package/",
        ":zip",
      ],
      patterns = [
        "!tests/.*",
        "!backfill/.*",
        "!.DS_Store",
        "!.*.md",
      ]
    }

  ]
```
We see that changes are being made in the pip of the container that is currently running as part of the CI/CD pipeline. If two or more pieces of this code run simultaneously the system files of Python/Pip can get corrupted and the deployment will fail..