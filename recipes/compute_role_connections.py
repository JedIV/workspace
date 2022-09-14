# -*- coding: utf-8 -*-
import dataiku
from dataiku import pandasutils as pdu
import pandas as pd
from boto_connections import Aws_Roles, get_boto3_iam_client

import boto3
from typing import Dict, List
from ast import literal_eval

# Read recipe inputs
role_groups = dataiku.Dataset("role_groups")
role_groups_df = role_groups.get_dataframe()
role_groups_df["groups"] = role_groups_df["groups"].apply(literal_eval)

rg_list = role_groups_df.to_dict('records')


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
roles = rg_list

dku_client = dataiku.api_client()

policy_map = role_generator.get_policies_for_roles(roles)
policy_list = role_generator.get_policy_role_list(policy_map)
full_list = role_generator.get_buckets_policy_role_list(policy_list)

roles = roles + full_list

clean_roles = []

for role in roles:

    if "bucket" in role:
        name = role["role"] + "_" + role["bucket"]
        bucket = role["bucket"]
    else:
        name = role["role"]
        bucket = ''
    
    role["arn"] = role_generator.get_arn_for_role(role["role"])
    role["connection"] = name
    params = {'credentialsMode': 'STS_ASSUME_ROLE',
      'defaultManagedPath': '/dataiku',
      'hdfsInterface': 'S3A',
      'encryptionMode': 'NONE',
      'chbucket': bucket,
      'switchToRegionFromBucket': True,
      'usePathMode': False,
      'stsRoleToAssume': role["arn"],
      'metastoreSynchronizationMode': 'NO_SYNC',
      'customAWSCredentialsProviderParams': [],
      'dkuProperties': [],
      'namingRule': {}}
    try:
        new_connection = dku_client.create_connection(name, type='EC2', params=params, usable_by='ALLOWED', allowed_groups= role["groups"])
        role["result"] = "success"
        definition = new_connection.get_definition()
        definition['detailsReadability'] = {'readableBy': 'ALLOWED', 'allowedGroups': role["groups"]}
        new_connection.set_definition(definition)
    except Exception as e: role["result"] = e


    clean_roles.append(role)

list_of_connections = dataiku.Dataset("role_connections")
list_of_connections.write_with_schema(pd.DataFrame(clean_roles))

