# import unittest
# import pandas as pd
# import sys
# from unittest.mock import patch, MagicMock

# sys.path.append("src/common/")
# sys.path.append("src/glue/clearbank_directdebit/")
# import api_client
# import clearbank_directdebit_mandates_to_s3_raw


# class TestDirectDebitFunctions(unittest.TestCase):

#     @patch('boto3.client')
#     @patch('awsglue.utils.getResolvedOptions')  # Mocking awsglue
#     def test_get_secret(self, mock_get_resolved_options, mock_boto_client):
#         # Setup mock for awsglue
#         mock_get_resolved_options.return_value = {
#             'ENV': 'test_env',
#             'CB_API_KEY': 'test_key',
#             'CB_BASE_URL': 'https://example.com'
#         }

#         # Setup mock for boto3
#         mock_secrets_manager = MagicMock()
#         mock_secrets_manager.get_secret_value.return_value = {'SecretString': 'test_secret_value'}
#         mock_boto_client.return_value = mock_secrets_manager

#         # Call the function
#         secret = clearbank_directdebit_mandates_to_s3_raw.get_secret('test_secret')

#         # Assert
#         self.assertEqual(secret, 'test_secret_value')
#         mock_boto_client.assert_called_once_with('secretsmanager')
#         mock_secrets_manager.get_secret_value.assert_called_once_with(SecretId='test_secret')

#     @patch('clearbank_directdebit_mandates_to_s3_raw.APIClient.get')
#     async def test_fetch_mandates(self, mock_api_client_get):
#         # Setup
#         mock_api_client_get.return_value = pd.DataFrame({'id': [1], 'name': ['test']})

#         # Call the function
#         mandates = await clearbank_directdebit_mandates_to_s3_raw.fetch_mandates(
#             cb_client=MagicMock(),
#             main_account_id='account_123',
#             page_number=1,
#             page_size=1000,
#             virtualAccountIds=[1, 2, 3],
#             max_retries=5
#         )

#         # Assert
#         self.assertIsInstance(mandates, pd.DataFrame)
#         self.assertEqual(len(mandates), 1)
#         mock_api_client_get.assert_called()

#     @patch('clearbank_directdebit_mandates_to_s3_raw.APIClient.get')
#     async def test_fetch_transactions(self, mock_api_client_get):
#         # Setup
#         mock_api_client_get.return_value = pd.DataFrame({'id': [1], 'amount': [100]})

#         # Call the function
#         transactions = await clearbank_directdebit_mandates_to_s3_raw.fetch_transactions(
#             cb_client=MagicMock(),
#             main_account_id='account_123',
#             cb_table='transactions',
#             cb_filter_object='filter',
#             page_number=1,
#             page_size=1000
#         )

#         # Assert
#         self.assertIsInstance(transactions, pd.DataFrame)
#         self.assertEqual(len(transactions), 1)
#         mock_api_client_get.assert_called()

#     @patch('clearbank_directdebit_mandates_to_s3_raw.raw_write_to_s3')
#     def test_upload_to_s3(self, mock_raw_write_to_s3):
#         # Setup
#         mock_raw_write_to_s3.return_value = True
#         df = pd.DataFrame({'id': [1], 'amount': [100]})

#         # Call the function
#         result = clearbank_directdebit_mandates_to_s3_raw.upload_to_s3(df, 'transactions', 'test_env')

#         # Assert
#         self.assertTrue(result)
#         mock_raw_write_to_s3.assert_called()

#     @patch('clearbank_directdebit_mandates_to_s3_raw.APIClient.get')
#     def test_get_total_pages(self, mock_api_client_get):
#         # Configure the mock response for two pages
#         mock_api_client_get.side_effect = [
#             [{'id': i} for i in range(1000)],  # First page with 1000 records
#             [{'id': i} for i in range(500)]    # Second page with 500 records
#         ]

#         # Create a MagicMock for cb_client, but its get method will use the mock_api_client_get
#         mock_client = MagicMock()
#         mock_client.get = mock_api_client_get  # Override the get method with the patched version

#         # Call the function with the patched client
#         total_pages = clearbank_directdebit_mandates_to_s3_raw.get_total_pages(
#             cb_client=mock_client,
#             main_account_id='account_123',
#             page_size=1000,
#             cb_table='transactions',
#             cb_filter_object='filter'
#         )

#         print(f"Total pages calculated: {total_pages}")

#         # Assert that the total_pages is 2
#         self.assertEqual(total_pages, 2)

#         # Ensure the mock API client was called
#         mock_api_client_get.assert_called()

#     @patch('clearbank_directdebit_mandates_to_s3_raw.APIClient.get')
#     async def test_fetch_mandates_in_batches(self, mock_api_client_get):
#         # Ensure mock returns a valid DataFrame
#         mock_api_client_get.return_value = pd.DataFrame({'id': [1], 'name': ['test']})

#         # Call the function
#         mandates = await clearbank_directdebit_mandates_to_s3_raw.fetch_mandates_in_batches(
#             cb_client=MagicMock(),
#             main_account_id='account_123',
#             page_number=1,
#             virtualAccountIds=[1, 2, 3, 4, 5],
#             batch_size=2,
#             max_retries=5
#         )

#         # Assert
#         self.assertIsInstance(mandates, pd.DataFrame)
#         self.assertEqual(len(mandates), 1)
#         mock_api_client_get.assert_called()


# if __name__ == '__main__':
#     unittest.main()
