from aws_cdk import Stack, RemovalPolicy, aws_lambda as _lambda, aws_apigateway as apigw
import aws_cdk as cdk
from aws_cdk import aws_s3 as s3
from constructs import Construct
from aws_cdk import aws_iam as iam
from aws_cdk.aws_lambda import LayerVersion, Code, Runtime, Architecture
from aws_cdk import aws_logs as logs


class CdkAppStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        bucket = s3.Bucket(
            self,
            "challenge-g-data-lake",
            bucket_name="challenge-g-data-lake",
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )

        lambda_role = iam.Role(
            self,
            "LambdaExecutionRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            description="Role to allow Lambda function to access S3 and Aurora RDS resources",
        )

        lambda_role.add_to_policy(
            iam.PolicyStatement(
                actions=["s3:*"],
                resources=[bucket.bucket_arn, bucket.bucket_arn + "/*"],
            )
        )

        lambda_role.add_to_policy(
            iam.PolicyStatement(
                actions=["rds-data:*", "secretsmanager:GetSecretValue"], resources=["*"]
            )
        )

        lambda_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents",
                ],
                resources=["arn:aws:logs:*:*:*"],
            )
        )

        sdk_lambda_layer = _lambda.LayerVersion.from_layer_version_arn(
            self,
            "AWSSDKLayer",
            layer_version_arn="arn:aws:lambda:us-east-1:336392948345:layer:AWSSDKPandas-Python312-Arm64:1",
        )

        custom_lambda_layer = LayerVersion(
            scope=self,
            id=f"custom_lambda_layer",
            code=Code.from_asset("lambda_layer/python.zip"),
            compatible_runtimes=[Runtime.PYTHON_3_12],
            compatible_architectures=[Architecture.ARM_64],
        )

        s3_to_rds_lambda_function = _lambda.Function(
            self,
            "s3_to_rds_lambda_function",
            function_name="s3_to_rds_lambda_function",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="s3_to_rds_lambda.lambda_handler",
            code=_lambda.Code.from_asset("src"),
            layers=[sdk_lambda_layer],
            memory_size=512,
            timeout=cdk.Duration.seconds(300),
            architecture=Architecture.ARM_64,
            role=lambda_role,
            log_retention=logs.RetentionDays.THREE_DAYS,
        )

        insert_data_lambda_function = _lambda.Function(
            self,
            "insert_data_lambda_function",
            function_name="insert_data_lambda_function",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="insert_data_lambda.lambda_handler",
            code=_lambda.Code.from_asset("src"),
            layers=[sdk_lambda_layer, custom_lambda_layer],
            memory_size=512,
            timeout=cdk.Duration.seconds(300),
            architecture=_lambda.Architecture.ARM_64,
            role=lambda_role,
            log_retention=logs.RetentionDays.THREE_DAYS,
        )

        s3_to_rds_log_group = logs.LogGroup(
            self,
            "s3ToRdsLogGroup",
            log_group_name=f"/aws/lambda/s3_to_rds_lambda_function",
            removal_policy=cdk.RemovalPolicy.DESTROY,
            retention=logs.RetentionDays.ONE_WEEK,
        )

        insert_data_log_group = logs.LogGroup(
            self,
            "InsertDataLogGroup",
            log_group_name=f"/aws/lambda/insert_data_lambda_function",
            removal_policy=cdk.RemovalPolicy.DESTROY,
            retention=logs.RetentionDays.ONE_WEEK,
        )

        s3_to_rds_lambda_function.node.add_dependency(s3_to_rds_log_group)
        insert_data_lambda_function.node.add_dependency(insert_data_log_group)

        api = apigw.RestApi(
            self,
            "data_operations",
            rest_api_name="DataOperationsAPI",
            description="API Gateway to interact with the RDS data",
        )

        integration = apigw.LambdaIntegration(insert_data_lambda_function)

        insert_data_resource = api.root.add_resource("insert_data")

        deployment = apigw.Deployment(self, "Deployment", api=api)

        dev_stage = apigw.Stage(
            self, "DevStage", deployment=deployment, stage_name="dev"
        )

        api.deployment_stage = dev_stage

        api_key = apigw.ApiKey(
            self, "ApiKey", api_key_name="chall_g_api_key", enabled=True
        )

        usage_plan = apigw.UsagePlan(
            self,
            "UsagePlan",
            name="BasicUsagePlan",
            api_stages=[apigw.UsagePlanPerApiStage(api=api, stage=dev_stage)],
            throttle=apigw.ThrottleSettings(rate_limit=10, burst_limit=2),
        )

        usage_plan.add_api_key(api_key)

        insert_data_resource.add_method("POST", integration, api_key_required=True)
