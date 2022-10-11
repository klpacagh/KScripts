import aws_cdk as cdk
from aws_cdk.aws_appflow import CfnFlow
from src.logging.salesforce_logs import Salesforce_Logs
from src.utils.helpers import pascal_case, hyphenate


class Salesforce:
    def __init__(self, scope: cdk.Stack, **kwargs) -> None:
        source = kwargs.get('source')
        sObjects = kwargs.get('sObjects', [])

        for sObj in sObjects:
            flow = self.__create_flow(scope, id=id, source=source, sObj=sObj)
            Salesforce_Logs(scope, flow_name=flow.flow_name,
                            id=scope.get_logical_id(flow))

    def __create_flow(self, scope: cdk.Stack, *, id: str, source: str, sObj: str) -> CfnFlow:
        formatted_sObj = hyphenate(sObj).lower()
        id = f'Sync{pascal_case(source)}{pascal_case(sObj)}'

        return CfnFlow(scope, id,
                       flow_name=f'{source}-{formatted_sObj}',
                       destination_flow_config_list=[CfnFlow.DestinationFlowConfigProperty(
                           connector_type='S3',
                           destination_connector_properties=CfnFlow.DestinationConnectorPropertiesProperty(
                               s3=CfnFlow.S3DestinationPropertiesProperty(
                                   bucket_name=cdk.Fn.import_value(
                                       'DATALAKE-RAW-BUCKET'),
                                   bucket_prefix=source,
                                   s3_output_format_config=CfnFlow.S3OutputFormatConfigProperty(
                                       aggregation_config=CfnFlow.AggregationConfigProperty(
                                           aggregation_type='None'
                                           ),
                                       file_type='PARQUET',
                                       prefix_config=CfnFlow.PrefixConfigProperty(
                                           prefix_format='DAY',
                                           prefix_type='PATH_AND_FILENAME'
                                           ),
                                       preserve_source_data_typing=False
                                       )
                                   ),
                               ),
                           )],
                       source_flow_config=CfnFlow.SourceFlowConfigProperty(
                           connector_profile_name=source,
                           connector_type='Salesforce',
                           source_connector_properties=CfnFlow.SourceConnectorPropertiesProperty(
                               salesforce=CfnFlow.SalesforceSourcePropertiesProperty(
                                   object=sObj,
                                   enable_dynamic_field_update=True,
                                   include_deleted_records=False
                                   )
                               ),
                           ),
                       tasks=[CfnFlow.TaskProperty(
                           source_fields=[],
                           task_type='Map_all',
                           task_properties=[CfnFlow.TaskPropertiesObjectProperty(
                               key='EXCLUDE_SOURCE_FIELDS_LIST',
                               value='[]'
                               )]
                           )],
                       trigger_config=CfnFlow.TriggerConfigProperty(
                           trigger_type='Scheduled',
                           trigger_properties=CfnFlow.ScheduledTriggerPropertiesProperty(
                               schedule_expression='cron(0 6 * * ? *)',
                               data_pull_mode='Complete'
                               )
                           )
                       )
