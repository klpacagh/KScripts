import aws_cdk as cdk
from aws_cdk.aws_logs import LogGroup, RetentionDays
from aws_cdk.aws_events import EventPattern, Rule
from aws_cdk.aws_events_targets import CloudWatchLogGroup


class Salesforce_Logs:
    def __init__(self, scope: cdk.Stack, *, flow_name: str, id: str) -> None:
        log_group = LogGroup(scope, f'{id}LogGroup',
                             log_group_name=f'/aws/appflow/{flow_name}-flow-logs',
                             retention=RetentionDays.INFINITE
                             )

        Rule(scope, f'{id}FlowMonitor',
             event_pattern=EventPattern(
                 source=['aws.appflow'],
                 detail_type=['AppFlow End Flow Run Report'],
                 detail={
                    'flow-name': [flow_name]
                     }
                 ),
             rule_name=f'{flow_name}-flow-monitor',
             targets=[CloudWatchLogGroup(log_group)]
             )
