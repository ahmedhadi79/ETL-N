WITH outbound_money_spend_with_card AS (
	SELECT parent_account_key,
		currency_code,
		SUM(cast(amount AS real)) AS amt
	FROM datalake_raw.deposit_transactions
	WHERE transaction_details_transaction_channel_id = 'Paymentology-FastLite'
		AND type = 'WITHDRAWAL'
	GROUP BY parent_account_key,
		currency_code
),
outbound_money_send_with_nomo AS (
	SELECT parent_account_key,
		currency_code,
		SUM(cast(amount AS real)) AS amt
	FROM datalake_raw.deposit_transactions
	WHERE transaction_details_transaction_channel_id IN (
			'CB_Withdrawal_FPS',
			'MBU_Withdrawal_FPS',
			'Wise_Local_Payments',
			'TW_Local_Payments',
			'BLME_USD_Withdrawal'
		)
		AND type = 'WITHDRAWAL'
	GROUP BY parent_account_key,
		currency_code
),
latest_balances AS (
	SELECT account_holder_key,
		account_type,
		encoded_key,
		cast(balances_total_balance AS double) AS balances_total_balance,
		id,
		currency_code,
		account_state,
		name as account,
		ROW_NUMBER() OVER (
			PARTITION BY account_holder_key,
			account_type,
			id
			ORDER BY CAST(
					From_iso8601_timestamp("last_modified_date") AS timestamp
				) DESC
		) AS rn_accounts,
		CAST(
			From_iso8601_timestamp("last_modified_date") AS timestamp
		) as "last_modified_date",
		CAST(
			From_iso8601_timestamp("approved_date") AS timestamp
		) as "approved_date",
		CAST(
			From_iso8601_timestamp("activation_date") AS timestamp
		) as "activation_date"
	FROM datalake_raw.deposit_accounts
),
latest_clients AS (
	SELECT encoded_key,
		id,
		state,
		ROW_NUMBER() OVER (
			PARTITION BY encoded_key,
			id
			ORDER BY last_modified_date DESC
		) AS rn_clients
	FROM datalake_raw.clients
),
latest_customers AS (
	SELECT user_id,
		dynamodb_new_image_updated_at_n,
		dynamodb_new_image_status_s,
		dynamodb_new_image_card_ordered_bool,
		ROW_NUMBER() OVER (
			PARTITION BY user_id
			ORDER BY dynamodb_new_image_updated_at_n DESC
		) AS rn_dynamo
	FROM (
			SELECT COALESCE(dynamodb_keys_id_s,dynamodb_key_id_s,dynamodb_new_image_id_s) as user_id ,
				dynamodb_new_image_updated_at_n,
				dynamodb_new_image_status_s,
				dynamodb_new_image_card_ordered_bool
				FROM datalake_raw.dynamo_scv_sls_customers
			GROUP BY COALESCE(dynamodb_keys_id_s,dynamodb_key_id_s,dynamodb_new_image_id_s),
				dynamodb_new_image_updated_at_n,
				dynamodb_new_image_status_s,
				dynamodb_new_image_card_ordered_bool
				) a
)   ,
card_statuses AS (
	SELECT dynamodb_new_image_user_id_s as id,
		ROW_NUMBER() OVER (
			PARTITION BY dynamodb_new_image_user_id_s
			ORDER BY dynamodb_new_image_updated_at_n DESC
		) as rn_card_status,
		dynamodb_new_image_state_s as card_state
	FROM datalake_raw.dynamo_sls_cards
),
joined AS (
	SELECT ft.amt AS mambu_outbound_card_transaction_amount,
		ot.amt AS mambu_outbound_nomo_transaction_amount,
		b.account AS mambu_account,
		b.id AS mambu_account_id,
		b.account_type AS mambu_account_type,
		b.balances_total_balance AS mambu_account_total_balance,
		b.encoded_key AS mambu_deposit_accounts_encoded_key,
		b.account_holder_key AS mambu_account_holder_key,
		b.currency_code AS mambu_account_currency_code,
		b.account_state AS mambu_account_state,
		b.last_modified_date AS mambu_account_last_modified_date,
		b.approved_date AS mambu_account_approved_date,
		b.activation_date AS mambu_account_activation_date,
		dynamo.user_id AS dynamo_user_key,
		c.state AS mambu_user_state,
		c.id AS mambu_client_id,
		c.encoded_key AS mambu_clients_encoded_key,
		dynamo.dynamodb_new_image_card_ordered_bool AS dynamo_card_ordered,
		COALESCE(NULLIF(cs.card_state, ''), 'UNKNOWN') AS dynamo_card_state
	FROM latest_customers dynamo
		LEFT JOIN latest_clients c ON c.id = dynamo.user_id
		LEFT JOIN latest_balances b ON b.account_holder_key = c.encoded_key
		LEFT JOIN outbound_money_spend_with_card ft ON ft.parent_account_key = b.encoded_key
		AND ft.currency_code = b.currency_code
		LEFT JOIN outbound_money_send_with_nomo ot ON ot.parent_account_key = b.encoded_key
		AND ft.currency_code = b.currency_code
		LEFT JOIN card_statuses cs ON CAST(cs.id as varchar) = c.id
	WHERE rn_clients = 1
		AND rn_dynamo = 1
		AND rn_accounts = 1
		AND rn_card_status = 1
)
SELECT *
FROM joined