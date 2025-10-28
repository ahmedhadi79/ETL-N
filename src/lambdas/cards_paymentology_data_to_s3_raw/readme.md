# Paymentology S3 ➜ SQS ➜ Lambda Copier

# Moves CSV files uploaded to a source S3 bucket into destination subfolders in a raw data lake bucket, based on filename patterns:

## _Fees_ → paymentology_fees/

## _Interchange_ → paymentology_interchange/

## _Presentments_ → paymentology_presentments/

The Lambda is triggered by SQS messages that wrap S3:ObjectCreated events. It is idempotent: if the destination object already exists, it skips copying.


## Filename → Folder Rules

Files must end with .csv (case-insensitive) or they are skipped.

Matching is by substring in the filename (not the full key):

Contains _Fees_ → fees

Contains _Interchange_ → interchange

Contains _Presentments_ → presentments

If none matches, the file is skipped (no copy); this is intentional to avoid mis-routing.


# BackFill Process
```
{
  "backfill": {
    "src_bucket": "sftp-replica-8435bcc1",
    "prefix": "prod-paymentology/",
    "start_date": "2021-06-14",
    "end_date": "2021-06-30"
  }
}
```