import unittest
from unittest.mock import MagicMock
import pandas as pd
from datetime import date, datetime, timezone
from pandas.testing import assert_frame_equal
import os
import sys

sys.path.append(os.path.abspath("../"))
from src.common.custom_functions import (
    camelcase_to_snake_case,
    get_salesforce_data,
    get_salesforce_df,
    contains_arabic,
)


class TestSalesforceAPI(unittest.TestCase):
    def test_multiple_pages(self):
        mock_api = MagicMock()
        mock_api.get.side_effect = [
            {"records": ["Record 1"], "nextRecordsUrl": "url2"},
            {"records": ["Record 2"], "nextRecordsUrl": "url3"},
            {"records": ["Record 3"]},
        ]
        data = {"records": ["Record 0"], "nextRecordsUrl": "url1"}
        result = get_salesforce_data(data, mock_api)
        self.assertEqual(
            result, [["Record 0"], ["Record 1"], ["Record 2"], ["Record 3"]]
        )

    def test_single_page(self):
        mock_api = MagicMock()
        mock_api.get.return_value = {"records": ["Record 1"]}
        data = {"records": ["Record 0"]}
        result = get_salesforce_data(data, mock_api)
        self.assertEqual(result, [["Record 0"]])

    def test_no_data(self):
        mock_api = MagicMock()
        mock_api.get.return_value = {"records": []}
        data = {"records": []}
        result = get_salesforce_data(data, mock_api)
        self.assertEqual(result, [[]])

    def test_api_failure(self):
        mock_api = MagicMock()
        mock_api.get.side_effect = Exception("API failure")
        data = {"records": ["Record 0"], "nextRecordsUrl": "url1"}
        with self.assertRaises(Exception):
            get_salesforce_data(data, mock_api)


class TestCamelcaseToSnakecase(unittest.TestCase):
    def test_camelcase_to_snakecase(self):
        df = pd.DataFrame({"FirstName": ["John", "Jane"], "LastName": ["Doe", "Smith"]})
        expected_df = pd.DataFrame(
            {"first_name": ["John", "Jane"], "last_name": ["Doe", "Smith"]}
        )

        transformed_df = camelcase_to_snake_case(df)

        self.assertTrue(expected_df.equals(transformed_df))


class TestContainsArabic(unittest.TestCase):
    def test_contains_arabic(self):
        self.assertTrue(contains_arabic("مرحبا"))  # Arabic text
        self.assertTrue(contains_arabic("مرحبا123"))  # Arabic text with numbers
        self.assertFalse(contains_arabic("Hello"))  # Non-Arabic text
        self.assertFalse(contains_arabic("123"))  # Non-Arabic text with numbers
        self.assertFalse(contains_arabic(""))  # Empty string
        self.assertTrue(contains_arabic("Helloمرحبا"))
        self.assertTrue(contains_arabic("مرحباHello"))
        self.assertTrue(contains_arabic("بِسْمِ اللَّهِ"))


class TestGetSalesForceDf(unittest.TestCase):
    def test_get_salesforce_df(self):
        # Sample data for testing
        sample_data = [
            {
                "attributes": {
                    "type": "Case",
                    "url": "/services/data/v60.0/sobjects/Case/5008E00000IrWzDQAV",
                },
                "Id": "5008E00000IrWzDQAV",
                "Case_Re_work_Reason__c": None,
                "CreatedDate": "2021-03-24T11:48:39.000+0000",
            },
            {
                "attributes": {
                    "type": "Case",
                    "url": "/services/data/v60.0/sobjects/Case/5008E00000IrgqgQAB",
                },
                "Id": "5008E00000IrgqgQAB",
                "Case_Re_work_Reason__c": None,
                "CreatedDate": "2021-03-30T09:45:12.000+0000",
            },
        ]

        # Expected DataFrame
        expected_df = pd.DataFrame(
            {
                "Id": ["5008E00000IrWzDQAV", "5008E00000IrgqgQAB"],
                "Case_Re_work_Reason__c": [None, None],
                "CreatedDate": [
                    "2021-03-24T11:48:39.000+0000",
                    "2021-03-30T09:45:12.000+0000",
                ],
                "date": [
                    date.today().strftime("%Y%m%d"),
                    date.today().strftime("%Y%m%d"),
                ],
            }
        )
        current_utc_time = datetime.now(timezone.utc)

        expected_df["timestamp_extracted"] = current_utc_time

        # Call the function with the test data
        actual_df = get_salesforce_df([sample_data])
        actual_df["timestamp_extracted"] = current_utc_time

        # Assert if the actual DataFrame is equal to the expected DataFrame
        assert_frame_equal(
            actual_df.reset_index(drop=True), expected_df.reset_index(drop=True)
        )
