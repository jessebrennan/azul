from dataclasses import (
    dataclass,
)
import importlib
import json
import shlex
from typing import (
    List,
)

from azul import (
    config,
)
from azul.deployment import (
    aws,
)
from azul.objects import (
    InternMeta,
)
from azul.terraform import (
    emit_tf,
)


@dataclass(frozen=True)
class Lambda:
    """
    Represents a AWS Lambda function fronted by an AWS API Gateway.
    """
    name: str  # the name of the Lambda, e.g. 'service'
    domains: List[str]  # a list of public domain names that the Lambda should be exposed at
    policy: str  # AWS Policy for the lambda function

    @classmethod
    def for_name(cls, name):
        policy_module = importlib.import_module(f'azul.{name}.lambda_iam_policy')
        return cls(name=name,
                   domains=[
                       config.api_lambda_domain(name),
                       *config.api_lambda_domain_aliases(name)
                   ],
                   policy=json.dumps(getattr(policy_module, 'policy')))


lambdas = [
    Lambda.for_name('indexer'),
    Lambda.for_name('service')
]


@dataclass(frozen=True)
class Zone(metaclass=InternMeta):
    """
    Represents a Route 53 hosted zone
    """
    slug: str  # the string to use to name the Terraform data source for the zone
    name: str  # the name of the zone

    @classmethod
    def for_domain(cls, domain):
        if domain.endswith(config.domain_name):
            # Any subdomain of the main domain for the current deployment is expected to be defined in a single zone
            # For some lesser deployments (like the `sandbox` or personal deployments), the subdomain may have a dot
            # in it and the main domain may be shared with other deployments (like the `dev` deployment).
            name = config.domain_name
        else:
            # Other subdomain are expected to be defined in the zone for their immediate parent domain.
            name = '.'.join(domain.split('.')[1:])
        assert name
        return cls(slug=name.replace('.', '_').replace('-', '_'),
                   name=name)


zones_by_domain = {
    domain: Zone.for_domain(domain)
    for lambda_ in lambdas
    for domain in lambda_.domains
}

emit_tf({
    "data": [
        {
            "aws_route53_zone": {
                zone.slug: {
                    "name": zone.name,
                    "private_zone": False
                }
            }
        } for zone in set(zones_by_domain.values())
    ],
    # Note that ${} references exist to interpolate a value AND express a dependency.
    "resource": [
        {
            "aws_api_gateway_deployment": {
                lambda_.name: {
                    "rest_api_id": "${module.chalice_%s.rest_api_id}" % lambda_.name,
                    "stage_name": config.deployment_stage
                }
            },
            "aws_api_gateway_base_path_mapping": {
                f"{lambda_.name}_{i}": {
                    "api_id": "${module.chalice_%s.rest_api_id}" % lambda_.name,
                    "stage_name": "${aws_api_gateway_deployment.%s.stage_name}" % lambda_.name,
                    "domain_name": "${aws_api_gateway_domain_name.%s_%i.domain_name}" % (lambda_.name, i)
                }
                for i, domain in enumerate(lambda_.domains)
            },
            "aws_api_gateway_domain_name": {
                f"{lambda_.name}_{i}": {
                    "domain_name": "${aws_acm_certificate.%s_%i.domain_name}" % (lambda_.name, i),
                    "certificate_arn": "${aws_acm_certificate_validation.%s_%i.certificate_arn}" % (lambda_.name, i)
                } for i, domain in enumerate(lambda_.domains)
            },
            "aws_acm_certificate": {
                f"{lambda_.name}_{i}": {
                    "domain_name": domain,
                    "validation_method": "DNS",
                    "provider": "aws.us-east-1",
                    # I tried using SANs for the alias domains (like the DRS domain) but Terraform kept swapping the
                    # zones, I think because the order of elements in `aws_acm_certificate.domain_validation_options`
                    # is not deterministic. The alternative is to use separate certs, one for each domain, the main
                    # one as well as for each alias.
                    #
                    "subject_alternative_names": [],
                    "lifecycle": {
                        "create_before_destroy": True
                    }
                } for i, domain in enumerate(lambda_.domains)
            },
            "aws_acm_certificate_validation": {
                f"{lambda_.name}_{i}": {
                    "certificate_arn": "${aws_acm_certificate.%s_%i.arn}" % (lambda_.name, i),
                    "validation_record_fqdns": [
                        "${aws_route53_record.%s_domain_validation_%i.fqdn}" % (lambda_.name, i)],
                    "provider": "aws.us-east-1"
                } for i, domain in enumerate(lambda_.domains)
            },
            "aws_route53_record": {
                **{
                    f"{lambda_.name}_domain_validation_{i}": {
                        **{
                            key: "${aws_acm_certificate.%s_%i.domain_validation_options.0.resource_record_%s}"
                                 % (lambda_.name, i, key) for key in ('name', 'type')
                        },
                        "zone_id": "${data.aws_route53_zone.%s.id}" % zones_by_domain[domain].slug,
                        "records": [
                            "${aws_acm_certificate.%s_%i.domain_validation_options.0.resource_record_value}" % (
                                lambda_.name, i)
                        ],
                        "ttl": 60
                    } for i, domain in enumerate(lambda_.domains)
                },
                **{
                    f"{lambda_.name}_{i}": {
                        "zone_id": "${data.aws_route53_zone.%s.id}" % zones_by_domain[domain].slug,
                        "name": "${aws_api_gateway_domain_name.%s_%i.domain_name}" % (lambda_.name, i),
                        "type": "A",
                        "alias": {
                            "name": "${aws_api_gateway_domain_name.%s_%i.cloudfront_domain_name}" % (lambda_.name, i),
                            "zone_id": "${aws_api_gateway_domain_name.%s_%i.cloudfront_zone_id}" % (lambda_.name, i),
                            "evaluate_target_health": True,
                        }
                    } for i, domain in enumerate(lambda_.domains)
                }
            },
            **(
                {
                    "aws_cloudwatch_log_group": {
                        lambda_.name: {
                            "name": "/aws/apigateway/" + config.qualified_resource_name(lambda_.name),
                            "retention_in_days": 1827,
                            "provisioner": {
                                "local-exec": {
                                    "command": ' '.join(map(shlex.quote, [
                                        "python",
                                        config.project_root + "/scripts/log_api_gateway.py",
                                        "${module.chalice_%s.rest_api_id}" % lambda_.name,
                                        config.deployment_stage,
                                        "${aws_cloudwatch_log_group.%s.arn}" % lambda_.name
                                    ]))
                                }
                            }
                        }
                    }
                } if config.enable_monitoring else {
                }
            ),
            "aws_iam_role": {
                lambda_.name: {
                    "name": config.qualified_resource_name(lambda_.name),
                    "assume_role_policy": json.dumps({
                        "Version": "2012-10-17",
                        "Statement": [
                            {
                                "Effect": "Allow",
                                "Action": "sts:AssumeRole",
                                "Principal": {
                                    "Service": "lambda.amazonaws.com"
                                }
                            },
                            *(
                                {
                                    "Effect": "Allow",
                                    "Action": "sts:AssumeRole",
                                    "Principal": {
                                        "AWS": f"arn:aws:iam::{account}:root"
                                    },
                                    # Wildcards are not supported in `Principal`, but they are in `Condition`
                                    "Condition": {
                                        "StringLike": {
                                            "aws:PrincipalArn": [f"arn:aws:iam::{account}:role/{role}"
                                                                 for role in roles]
                                        }
                                    }
                                }
                                for account, roles in config.external_lambda_role_assumptors.items()
                            )
                        ]
                    }),
                    **aws.permissions_boundary_tf
                }
            },
            "aws_iam_role_policy": {
                lambda_.name: {
                    "name": lambda_.name,
                    "policy": lambda_.policy,
                    "role": "${aws_iam_role.%s.id}" % lambda_.name
                },
            }
        } for lambda_ in lambdas
    ]
})
