import dataiku
from dataiku import pandasutils as pdu
import pandas as pd
from boto_connections import Aws_Roles, get_boto3_iam_client

import boto3
from typing import Dict, List

# set connection with role to load bucket information here.
# the role should have the following policy attached:
#{
#    "Version": "2012-10-17",
#    "Statement": [
#        {
#            "Sid": "VisualEditor0",
#            "Effect": "Allow",
#            "Action": [
#                "iam:GetRole",
#                "iam:GetPolicyVersion",
#                "iam:GetPolicy",
#                "iam:ListAttachedRolePolicies",
#                "iam:ListRoles",
#                "iam:ListRolePolicies"
#            ],
#            "Resource": "*"
#        }
#    ]
#}

client = get_boto3_iam_client("s3-managed")

role_generator = Aws_Roles(client)

# list of roles and the groups associated with each role. You can enter them here, or load them as an input dataframe
roles = [{'role':'se-sandbox-s3-access-role', 'groups': ['administrators'] }, {'role':'jed-limited-s3-role', 'groups': ['administrators'] }]


dku_client = dataiku.api_client()

policy_map = role_generator.get_policies_for_roles(roles)
policy_list = role_generator.get_policy_role_list(policy_map)
full_list = role_generator.get_buckets_policy_role_list(policy_list)

clean_roles = []
for role in roles:
    role["arn"] = role_generator.get_arn_for_role(role["role"])
    role["connection"] = role["role"]
    params = {'credentialsMode': 'STS_ASSUME_ROLE',
      'defaultManagedPath': '/dataiku',
      'hdfsInterface': 'S3A',
      'encryptionMode': 'NONE',
      'switchToRegionFromBucket': True,
      'usePathMode': False,
      'stsRoleToAssume': role["arn"],
      'metastoreSynchronizationMode': 'NO_SYNC',
      'customAWSCredentialsProviderParams': [],
      'dkuProperties': [],
      'namingRule': {}}
    try:
        new_connection = dku_client.create_connection(role["role"], type='EC2', params=params, usable_by='ALLOWED', allowed_groups= role["groups"])
        role["result"] = "success"
    except Exception as e: role["result"] = e
    clean_roles.append(role)
        

for role_bucket in full_list:
    print(role_bucket)
    role_bucket["connection"] = role_bucket["role"] + "_" + role_bucket["bucket"]
    params = {'credentialsMode': 'STS_ASSUME_ROLE',
      'defaultManagedPath': '/dataiku',
      'hdfsInterface': 'S3A',
      'encryptionMode': 'NONE',
      'chbucket': role_bucket['bucket'],
      'switchToRegionFromBucket': True,
      'usePathMode': False,
      'stsRoleToAssume': role_bucket['arn'],
      'metastoreSynchronizationMode': 'NO_SYNC',
      'customAWSCredentialsProviderParams': [],
      'dkuProperties': [],
      'namingRule': {}}
    try:
        new_connection = dku_client.create_connection(role_bucket["connection"], type='EC2', params=params, usable_by='ALLOWED', allowed_groups=role_bucket['groups'])
        role_bucket["result"] = "success"
    except Exception as e: role_bucket["result"] = e

    clean_roles.append(role_bucket)


list_of_connections = dataiku.Dataset("list_of_connections")
list_of_connections.write_with_schema(pd.DataFrame(clean_roles))