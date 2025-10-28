select customer_reference_id__c from datalake_raw.salesforce_cases
where  status='Documents Requested'
and type='High risk manual review'