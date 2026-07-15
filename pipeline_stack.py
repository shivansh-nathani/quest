import aws_cdk as cdk
from aws_cdk import (
    Stack,
    Duration,
    RemovalPolicy,
    aws_lambda as _lambda,
    aws_s3 as s3,
    aws_dynamodb as dynamodb,
    aws_sqs as sqs,                                  # NEW IMPORT
    aws_lambda_event_sources as event_sources,       # NEW IMPORT
    aws_s3_notifications as s3n,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
    aws_events as events,
    aws_events_targets as targets,
)
from constructs import Construct

class DataPipelineStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)



        state_table = dynamodb.Table.from_table_name(
            self, "BLSSyncStateTable",
            "bls_sync_state"
        )
        data_bucket = s3.Bucket.from_bucket_name(
            self, "DataPipelineLandingBucket",
            "shvnsh-rearc-quest" 
        )
        lambda_1 = _lambda.Function(self, "Sync2S3Function", runtime=_lambda.Runtime.PYTHON_3_11, handler="sync2S3.handler", code=_lambda.Code.from_asset("lambda/sync2s3"), memory_size=1024, timeout=Duration.minutes(10), environment={"BUCKET_NAME": data_bucket.bucket_name, "TABLE_NAME": state_table.table_name})
        lambda_2 = _lambda.Function(self, "ApiDataStreamFunction", runtime=_lambda.Runtime.PYTHON_3_11, handler="api_data.handler", code=_lambda.Code.from_asset("lambda/api_data"), timeout=Duration.minutes(5), environment={"BUCKET_NAME": data_bucket.bucket_name})
        
        lambda_3 = _lambda.Function(
            self, "PandasAnalysisFunction",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="analysis.handler",
            code=_lambda.Code.from_asset("lambda/analysis"),
            memory_size=1024, 
            timeout=Duration.minutes(5),
            environment={
                "BUCKET_NAME": data_bucket.bucket_name
            }
        )

        state_table.grant_read_write_data(lambda_1)
        data_bucket.grant_read_write(lambda_1)
        data_bucket.grant_write(lambda_2)
        data_bucket.grant_read_write(lambda_3)

        # --- THE NEW SQS REQUIREMENT ---

        # 1. Create the SQS Queue
        # Note: The visibility timeout MUST be greater than or equal to the Lambda timeout (5 minutes)
        analysis_queue = sqs.Queue(
            self, "AnalysisTriggerQueue",
            visibility_timeout=Duration.minutes(6) 
        )

        # 2. Tell S3 to send a message to the SQS Queue when a DataUSA file is uploaded
        data_bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3n.SqsDestination(analysis_queue),
            s3.NotificationKeyFilter(prefix="data/datausa/") 
        )

        # 3. Tell Lambda 3 to consume messages from the SQS Queue
        lambda_3.add_event_source(
            event_sources.SqsEventSource(analysis_queue, batch_size=1)
        
        )

        lambda_3.add_environment("BUCKET_NAME", data_bucket.bucket_name)    
        # -------------------------------

        task_1 = tasks.LambdaInvoke(self, "RunBLSSync", lambda_function=lambda_1, payload_response_only=True)
        task_2 = tasks.LambdaInvoke(self, "RunDataUSAStream", lambda_function=lambda_2, payload_response_only=True)
        state_machine = sfn.StateMachine(self, "DailyDataPipeline", definition_body=sfn.DefinitionBody.from_chainable(task_1.next(task_2)), timeout=Duration.minutes(20))
        events.Rule(self, "DailyExecutionRule", schedule=events.Schedule.cron(minute="0", hour="6"), targets=[targets.SfnStateMachine(state_machine)])