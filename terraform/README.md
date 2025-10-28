# Sandbox Local dev

## Login to AWS Devops account and role
Run the following command:
```bash
aws configure sso --profile bb2-tf-devops
```
**Warning: Don't specify session name.**
For SSO start URL enter: `https://bb2.awsapps.com/start#/`.
For SSO region enter: `eu-west-2`.
Click `Allow` in the browser popup.

Then, in your command line, select `bb2-tech-devops` account(`521333308695`) and then `aws-tfdev-devops` role.

Note: If you don't have/ don't see this role, then you need to request this role by raising an MR in this repo:

https://gitlab.com/bb2-bank/self-service/azuread-config/-/blob/master/terraform/aws-devops.tf#L61

## Export Vars
Regarding External IDs vars, You should contact with DevOps team to receive these credentials.
```bash
export TF_VAR_external_id= < TF_VAR_external_id >
export TF_VAR_project_url=https://gitlab.com/bb2-bank/data-lake/data-lake-etl
export TF_RECREATE_MISSING_LAMBDA_PACKAGE=false
export TF_VAR_resource_management_iam_role="bb2-pipeline-automation-write"
export AWS_PROFILE=bb2-tf-devops
export TF_VAR_logging_external_id= < TF_VAR_logging_external_id >
```

## Run TF
```bash
cd data-lake-etl/terraform
terraform init -backend-config="../config/sandbox/backend.hcl"
terraform plan -var-file="../config/sandbox/terraform.tfvars"
terraform apply -var-file="../config/sandbox/terraform.tfvars"
```

If you need to only test one lambda you could do the following instead:

```bash
terraform plan -var-file="../config/sandbox/terraform.tfvars" -target module.lambda_mambutos3
terraform apply -var-file="../config/sandbox/terraform.tfvars" -target module.lambda_mambu_datalake_reconciliation
```
