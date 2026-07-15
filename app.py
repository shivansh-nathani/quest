#!/usr/bin/env python3
import aws_cdk as cdk

# Import the stack class we created in pipeline_stack.py
from pipeline_stack import DataPipelineStack

app = cdk.App()

# Instantiate the stack
DataPipelineStack(
    app, 
    "DataPipelineStack",
    # If you need to specify a specific AWS account or region, you can uncomment and add this:
    # env=cdk.Environment(account='123456789012', region='us-east-1'),
)

app.synth()