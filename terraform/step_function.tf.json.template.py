import json

from azul import (
    config,
)
from azul.deployment import (
    aws,
)
from azul.modules import (
    load_app_module,
)
from azul.terraform import (
    emit_tf,
)


def cart_item_states():
    return {
        "WriteBatch": {
            "Type": "Task",
            "Resource": aws.get_lambda_arn(config.service_name, config.cart_item_write_lambda_basename),
            "Next": "NextBatch",
            "ResultPath": "$.write_result"
        },
        "NextBatch": {
            "Type": "Choice",
            "Choices": [
                {
                    "Variable": "$.write_result.count",
                    "NumericEquals": 0,
                    "Next": "SuccessState",
                }
            ],
            "Default": "WriteBatch"
        },
        "SuccessState": {
            "Type": "Succeed"
        }
    }


service = load_app_module('service')

emit_tf({
    "resource": {
        "aws_iam_role": {
            "states": {
                "name": config.qualified_resource_name("statemachine"),
                "assume_role_policy": json.dumps({
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Sid": "",
                            "Effect": "Allow",
                            "Principal": {
                                "Service": "states.amazonaws.com"
                            },
                            "Action": "sts:AssumeRole"
                        }
                    ]
                }),
                **aws.permissions_boundary_tf
            }
        },
        "aws_iam_role_policy": {
            "states": {
                "name": config.qualified_resource_name("statemachine"),
                "role": "${aws_iam_role.states.id}",
                "policy": json.dumps({
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Action": [
                                "lambda:InvokeFunction"
                            ],
                            "Resource": [
                                aws.get_lambda_arn(config.service_name, service.generate_manifest.lambda_name),
                                aws.get_lambda_arn(config.service_name, config.cart_item_write_lambda_basename),
                                aws.get_lambda_arn(config.service_name, config.cart_export_dss_push_lambda_basename)
                            ]
                        }
                    ]
                })
            }
        },
        "aws_sfn_state_machine": {
            "manifest": {
                "name": config.state_machine_name(service.generate_manifest.lambda_name),
                "role_arn": "${aws_iam_role.states.arn}",
                "definition": json.dumps({
                    "StartAt": "WriteManifest",
                    "States": {
                        "WriteManifest": {
                            "Type": "Task",
                            "Resource": aws.get_lambda_arn(config.service_name, service.generate_manifest.lambda_name),
                            "End": True
                        }
                    }
                }, indent=2)
            },
            "cart_item": {
                "name": config.cart_item_state_machine_name,
                "role_arn": "${aws_iam_role.states.arn}",
                "definition": json.dumps({
                    "StartAt": "WriteBatch",
                    "States": cart_item_states()
                }, indent=2)
            },
            "cart_export": {
                "name": config.cart_export_state_machine_name,
                "role_arn": "${aws_iam_role.states.arn}",
                "definition": json.dumps({
                    "StartAt": "SendToCollectionAPI",
                    "States": {
                        "SendToCollectionAPI": {
                            "Type": "Task",
                            "Resource": aws.get_lambda_arn(config.service_name,
                                                           config.cart_export_dss_push_lambda_basename),
                            "Next": "NextBatch",
                            "ResultPath": "$"
                        },
                        "NextBatch": {
                            "Type": "Choice",
                            "Choices": [
                                {
                                    "Variable": "$.resumable",
                                    "BooleanEquals": False,
                                    "Next": "SuccessState"
                                }
                            ],
                            "Default": "SendToCollectionAPI"
                        },
                        "SuccessState": {
                            "Type": "Succeed"
                        }
                    }
                }, indent=2)
            }
        }
    }
})
