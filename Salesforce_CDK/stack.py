import aws_cdk as cdk
from src.flows.salesforce import Salesforce


class SyncSalesforceDatalake(cdk.Stack):
    def __init__(self, scope: cdk.App, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        Salesforce(self, source='salesforce-career-services',
                   sObjects=['Contact', 'Lead', 'Account', 'Opportunity']
                   )
        Salesforce(self, source='salesforce-championships',
                   sObjects=['Contact', 'Lead', 'Account', 'Opportunity']
                   )
