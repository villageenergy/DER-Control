# DER-Control
DER-Control

This code reads the stream data coming from representative DER and stores it in s3.

A comprehensive .yaml file containing the template file for DER Control Test scenarios was created. Two main resources are created by creting this stack:

1- A SageMaker Instance (DERControlInstance-XXXXXXXXXX), which is connected to a Github source for resource codes
2- A Kinesis Analytics Application (RepresentativeDERControl), which runs the sql code and outputs the stream needed for the SageMaker to processAll the resources are shared in the following Github Repo:
 
[derControlCode](DERControlTestScenarios-Prod.ipynb)

Some parameters are already set up to a default value. While creating a stack out of template file in AWS Cloudformation, they can be altered.

DERInputStream:	DERInputStream	
DEROutputStream:	DEROutputStream	-
InstanceType:	ml.t2.medium	-
RoleArn:	arn:aws:iam::440616111601:role/service-role/AmazonSageMaker-ExecutionRole-20200522T141912	-
deviceId:	demo-dss-001	-
microgridId:	IN-demo-dss-0001
