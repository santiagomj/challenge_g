import aws_cdk as cdk

from cdk_stacks.cdk_app_stack import CdkAppStack


app = cdk.App()
CdkAppStack(app, "CdkAppStack")

app.synth()
