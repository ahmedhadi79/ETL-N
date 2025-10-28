# Running
- this script depends in three layers ,we can find surrencycloud zipped file under this location "s3://bb2-sandbox-datalake-raw/layer/currency_cloud_layer.zip" in sandbox account
- we mange these  layers from terraform.

## A. Set up AWS SSO profile and S3 bucket
- This step is required to each env to get profile setting up and export result bucket for files 
```
    aws configure sso --profile bb2-prod-admin
    export S3_RAW=bb2-prod-datalake-raw
```

# Script  main module 
- this function mainly load data from currncy cloud website in form of these tables
{
    balance,
    accounts,
    beneficiaries,
    contacts,
    conversions,
    payments,
    transactions,
    transfers,
}

- try to get the appropriate schema for the ingested data  , and store the data in s3 and with the schema in Athena
files in s3 should start with currency cloud_ and Athena table the same.

- if we can't get the schema, we will store the data as one object in s3
files in s3 should start with currency cloud_ and ending with _fallback.

# final Output
- the final output of the data returned from currencycloud
1- in S3 bucket bb2-sandbox-datalake-raw "sandbox account" , we can search with "currencycloud_" as prefix  and we will find the output folders
2- also theoupt written in athena uder "datalake_raw" database ,we can search with "currencycloud_" as prefix and we will find all tables
