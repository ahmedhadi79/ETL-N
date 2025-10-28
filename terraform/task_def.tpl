{
    "taskDefinition": {
        "taskDefinitionArn": "arn:aws:ecs:eu-west-2:414717704904:task-definition/datalake-sandbox-dynamo-backfill-s3:3",
        "containerDefinitions": [
            {
                "name": "datalake-sandbox-dynamo-backfill-s3",
                "image": "414717704904.dkr.ecr.eu-west-2.amazonaws.com/datalake-sandbox-dynamo-backfill-s3:latest",
                "cpu": 0,
                "portMappings": [],
                "essential": true,
                "environment": [
                    {
                        "name": "S3_RAW",
                        "value": ${s3_raw}
                    },
                    {
                        "name": "DYNAMODB_CUSTOMERS",
                        "value": ${dynamodb_customers}
                    },
                    {
                        "name": "DYNAMODB_BACKFILL_TS",
                        "value": ${dynamodb_backfill_ts}
                    }
                ],
                "mountPoints": [],
                "volumesFrom": [],
                "logConfiguration": {
                    "logDriver": "awslogs",
                    "options": {
                        "awslogs-group": "/ecs/datalake-sandbox-dynamo-backfill-s3",
                        "awslogs-region": "eu-west-2",
                        "awslogs-stream-prefix": "ecs"
                    }
                }
            }
        ],
        "family": "datalake-sandbox-dynamo-backfill-s3",
        "taskRoleArn": "arn:aws:iam::414717704904:role/data_lake_iam_for_fargate",
        "executionRoleArn": "arn:aws:iam::414717704904:role/data_lake_iam_for_fargate",
        "networkMode": "awsvpc",
        "revision": 3,
        "volumes": [],
        "status": "ACTIVE",
        "requiresAttributes": [
            {
                "name": "com.amazonaws.ecs.capability.logging-driver.awslogs"
            },
            {
                "name": "ecs.capability.execution-role-awslogs"
            },
            {
                "name": "com.amazonaws.ecs.capability.ecr-auth"
            },
            {
                "name": "com.amazonaws.ecs.capability.docker-remote-api.1.19"
            },
            {
                "name": "com.amazonaws.ecs.capability.task-iam-role"
            },
            {
                "name": "ecs.capability.execution-role-ecr-pull"
            },
            {
                "name": "com.amazonaws.ecs.capability.docker-remote-api.1.18"
            },
            {
                "name": "ecs.capability.task-eni"
            }
        ],
        "placementConstraints": [],
        "compatibilities": [
            "EC2",
            "FARGATE"
        ],
        "requiresCompatibilities": [
            "FARGATE"
        ],
        "cpu": "4096",
        "memory": "30720",
        "registeredAt": "2021-07-22T18:37:30.514000+03:00",
        "registeredBy": "arn:aws:sts::414717704904:assumed-role/PowerUser/Andreas.Adamides@bb2.tech"
    }
}
