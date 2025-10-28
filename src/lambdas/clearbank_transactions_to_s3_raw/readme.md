# Project Overview

Clear Bank access detalis and table inforamtion.

# Environment Variables

	# PROD
	•	baseUrl: https://institution-api.clearbank.co.uk
	•	mainAccountId: 0033761c-700c-4797-923e-dc92fcad0d27
	# Beta
	•	baseUrl: https://institution-api-sim.clearbank.co.uk
	•	mainAccountId: b5e6dd1f-f200-438c-a726-a27d1354f5c9
	# Alpha
	•	baseUrl: https://institution-api-sim.clearbank.co.uk
	•	mainAccountId: a4f5d7cf-83ab-4690-a029-9016ff275095

# Running

	•	Cron scheduler.

# File Structure
    .
    ├── clearbank_data_to_s3_raw.py   # Transactions


# Tables Information
|Table Name|Glue - Job Name|Target|Schedule|
|:--------|:-------|:-----:|--------:|
|datalake_raw.cb_transactions|datalake-etl-clearbank-transactions-tos3raw|Datalake Raw|30 00 * * ? *|
