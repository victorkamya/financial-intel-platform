"""
Financial Intelligence Platform — imperative deploy script.
boto3 (AWS) + databricks-sdk (existing us-west-2 workspace/metastore).
Run: python deploy.py
"""

import io
import json
import os
import random
import string
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import time

import boto3
from botocore.exceptions import ClientError
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import PermissionDenied
from dotenv import load_dotenv

load_dotenv()  # never overrides vars already set in the shell session

# ─────────────────────────────────────────────────────────────────────────────
# ENV VAR VALIDATION — fail fast, before touching any API
# ─────────────────────────────────────────────────────────────────────────────

REQUIRED_ENV_VARS = [
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "DATABRICKS_HOST",
    "DATABRICKS_CLIENT_ID",
    "DATABRICKS_CLIENT_SECRET",
    "UC_TRUST_ACCOUNT_ID",  # deliberately NOT named DATABRICKS_ACCOUNT_ID —
    # the SDK auto-detects that name and switches to account-level OAuth,
    # breaking workspace-level auth. We only need this as a plain string
    # for the Unity Catalog IAM trust policy's ExternalId, not for auth.
    "DATABRICKS_METASTORE_ID",
    "ALPACA_API_KEY",
    "ALPACA_SECRET_KEY",
]


def require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(
            f"Missing required environment variable: {name}. "
            f"Set it in the terminal before running this script."
        )
    return value


for _var in REQUIRED_ENV_VARS:
    require_env(_var)

UC_TRUST_ACCOUNT_ID = require_env("UC_TRUST_ACCOUNT_ID")
DATABRICKS_METASTORE_ID = require_env("DATABRICKS_METASTORE_ID")

# ─────────────────────────────────────────────────────────────────────────────
# NAMING — project prefix + persisted random suffix
# ─────────────────────────────────────────────────────────────────────────────

PROJECT_NAME = "financial-intel-platform"
ENVIRONMENT = "dev"
OWNER_EMAIL = "vic1771@hotmail.com"

OUTPUTS_PATH = Path(__file__).parent / "outputs.json"


def load_or_init_outputs() -> dict:
    if OUTPUTS_PATH.exists():
        with open(OUTPUTS_PATH, "r") as f:
            data = json.load(f)
            data.setdefault("log", [])
            return data

    suffix = "".join(random.choices(string.ascii_lowercase + string.digits, k=6))
    data = {"suffix": suffix, "resources": {}, "log": []}
    save_outputs(data)
    return data


def save_outputs(data: dict) -> None:
    with open(OUTPUTS_PATH, "w") as f:
        json.dump(data, f, indent=2)


def record_resource(outputs: dict, key: str, value) -> None:
    outputs["resources"][key] = value
    save_outputs(outputs)


def log_event(outputs: dict, step: str, event: str, detail: str = "") -> None:
    outputs["log"].append({
        "ts": datetime.now(timezone.utc).isoformat(),
        "step": step,
        "event": event,
        "detail": detail,
    })
    save_outputs(outputs)


OUTPUTS = load_or_init_outputs()
SUFFIX = OUTPUTS["suffix"]
PREFIX = f"{PROJECT_NAME}-{SUFFIX}"

print(f"Using prefix: {PREFIX}  (suffix persisted in {OUTPUTS_PATH.name} — reused on re-run)")

# ─────────────────────────────────────────────────────────────────────────────
# REGIONS
# ─────────────────────────────────────────────────────────────────────────────

PRIMARY_REGION = "us-west-2"
DR_REGION = "us-east-1"

# ─────────────────────────────────────────────────────────────────────────────
# TAGS
# ─────────────────────────────────────────────────────────────────────────────

COMMON_TAGS = {
    "Project": PROJECT_NAME,
    "Environment": ENVIRONMENT,
    "ManagedBy": "python-script",
    "Owner": OWNER_EMAIL,
}


def aws_tag_list(tags: dict) -> list:
    return [{"Key": k, "Value": v} for k, v in tags.items()]


def create_bucket_in_region(client, bucket_name: str, region: str) -> None:
    # us-east-1 is S3's implicit default region — CreateBucketConfiguration
    # must be omitted entirely there, or the API rejects it with
    # InvalidLocationConstraint. Every other region requires it.
    if region == "us-east-1":
        client.create_bucket(Bucket=bucket_name)
    else:
        client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": region},
        )


# ─────────────────────────────────────────────────────────────────────────────
# CLIENTS
# ─────────────────────────────────────────────────────────────────────────────

s3_primary = boto3.client("s3", region_name=PRIMARY_REGION)
s3_dr = boto3.client("s3", region_name=DR_REGION)
kinesis = boto3.client("kinesis", region_name=PRIMARY_REGION)
secretsmanager = boto3.client("secretsmanager", region_name=PRIMARY_REGION)
iam = boto3.client("iam")
cloudwatch = boto3.client("cloudwatch", region_name=PRIMARY_REGION)
lambda_client = boto3.client("lambda", region_name=PRIMARY_REGION)

w = WorkspaceClient()  # reads DATABRICKS_HOST / DATABRICKS_CLIENT_ID / DATABRICKS_CLIENT_SECRET

# ─────────────────────────────────────────────────────────────────────────────
# STEP 2: S3 MEDALLION BUCKETS
# ─────────────────────────────────────────────────────────────────────────────

BUCKET_SPECS = {
    "bronze": {"versioned": True},
    "silver": {"versioned": True},
    "gold": {"versioned": True},
    "mlflow": {"versioned": False},
    "unity_catalog": {"versioned": False},
}


def bucket_exists(client, name: str) -> bool:
    try:
        client.head_bucket(Bucket=name)
        return True
    except ClientError:
        return False


def create_medallion_buckets() -> dict:
    print("\n--- Step 2: S3 medallion buckets ---")
    bucket_names = {}

    for key, spec in BUCKET_SPECS.items():
        bucket_name = f"{PREFIX}-{key.replace('_', '-')}"
        bucket_names[key] = bucket_name

        if bucket_exists(s3_primary, bucket_name):
            print(f"  [skip] bucket already exists: {bucket_name}")
        else:
            create_bucket_in_region(s3_primary, bucket_name, PRIMARY_REGION)
            print(f"  [ok]   created bucket: {bucket_name}")

        s3_primary.put_public_access_block(
            Bucket=bucket_name,
            PublicAccessBlockConfiguration={
                "BlockPublicAcls": True,
                "BlockPublicPolicy": True,
                "IgnorePublicAcls": True,
                "RestrictPublicBuckets": True,
            },
        )

        if spec["versioned"]:
            s3_primary.put_bucket_versioning(
                Bucket=bucket_name,
                VersioningConfiguration={"Status": "Enabled"},
            )

        s3_primary.put_bucket_tagging(
            Bucket=bucket_name,
            Tagging={"TagSet": aws_tag_list(COMMON_TAGS)},
        )

        record_resource(OUTPUTS, f"{key}_bucket", bucket_name)

    return bucket_names


# ─────────────────────────────────────────────────────────────────────────────
# STEP 3: KINESIS STREAM
# ─────────────────────────────────────────────────────────────────────────────

def stream_exists(name: str) -> bool:
    try:
        kinesis.describe_stream_summary(StreamName=name)
        return True
    except kinesis.exceptions.ResourceNotFoundException:
        return False


def create_kinesis_stream() -> str:
    print("\n--- Step 3: Kinesis stream ---")
    stream_name = f"{PREFIX}-market-data"

    if stream_exists(stream_name):
        print(f"  [skip] stream already exists: {stream_name}")
    else:
        kinesis.create_stream(
            StreamName=stream_name,
            ShardCount=2,
            StreamModeDetails={"StreamMode": "PROVISIONED"},
        )
        waiter = kinesis.get_waiter("stream_exists")
        waiter.wait(StreamName=stream_name)
        print(f"  [ok]   created stream: {stream_name}")

    summary = kinesis.describe_stream_summary(StreamName=stream_name)["StreamDescriptionSummary"]
    stream_arn = summary["StreamARN"]

    kinesis.add_tags_to_stream(StreamName=stream_name, Tags=COMMON_TAGS)

    record_resource(OUTPUTS, "kinesis_stream_name", stream_name)
    record_resource(OUTPUTS, "kinesis_stream_arn", stream_arn)

    return stream_name


# ─────────────────────────────────────────────────────────────────────────────
# STEP 4: SECRETS MANAGER
# ─────────────────────────────────────────────────────────────────────────────

SEC_EDGAR_USER_AGENT = f"Victor Kamya {OWNER_EMAIL}"


def secret_exists(secret_id: str) -> bool:
    try:
        secretsmanager.describe_secret(SecretId=secret_id)
        return True
    except secretsmanager.exceptions.ResourceNotFoundException:
        return False


def create_sec_edgar_secret() -> str:
    print("\n--- Step 4: Secrets Manager ---")
    secret_id = f"{PREFIX}/sec-edgar-user-agent"

    if secret_exists(secret_id):
        print(f"  [skip] secret already exists: {secret_id}")
    else:
        secretsmanager.create_secret(
            Name=secret_id,
            SecretString=json.dumps({"user_agent": SEC_EDGAR_USER_AGENT}),
            Tags=aws_tag_list(COMMON_TAGS),
        )
        print(f"  [ok]   created secret: {secret_id}")

    secret_arn = secretsmanager.describe_secret(SecretId=secret_id)["ARN"]
    record_resource(OUTPUTS, "sec_edgar_secret_arn", secret_arn)

    return secret_arn


# ─────────────────────────────────────────────────────────────────────────────
# STEP 5: IAM ROLES
# ─────────────────────────────────────────────────────────────────────────────

def role_exists(role_name: str) -> bool:
    try:
        iam.get_role(RoleName=role_name)
        return True
    except iam.exceptions.NoSuchEntityException:
        return False


def create_role_if_not_exists(role_name: str, trust_policy: dict) -> str:
    if role_exists(role_name):
        print(f"  [skip] role already exists: {role_name}")
    else:
        try:
            iam.create_role(
                RoleName=role_name,
                AssumeRolePolicyDocument=json.dumps(trust_policy),
                Tags=aws_tag_list(COMMON_TAGS),
            )
            print(f"  [ok]   created role: {role_name}")
        except iam.exceptions.EntityAlreadyExistsException:
            print(f"  [skip] role already exists (race): {role_name}")
    return iam.get_role(RoleName=role_name)["Role"]["Arn"]


def put_inline_policy(role_name: str, policy_name: str, policy_doc: dict) -> None:
    iam.put_role_policy(
        RoleName=role_name,
        PolicyName=policy_name,
        PolicyDocument=json.dumps(policy_doc),
    )


def create_iam_roles(bucket_names: dict) -> dict:
    print("\n--- Step 5: IAM roles ---")
    log_event(OUTPUTS, "step5", "start", "creating s3-replication-role")

    # NOTE: the unity-catalog-role originally planned here is deliberately
    # skipped. Its trust policy needs Databricks' storage-credential-specific
    # ExternalId, which only exists AFTER a Unity Catalog storage credential
    # object is created — and that step was already deferred (the "faster
    # option 2" decision: catalogs/schemas use the metastore's default
    # managed location, not these buckets). Also, "databricks.amazonaws.com"
    # is not a valid AWS service principal — Databricks assumes roles
    # cross-account via its own AWS account (arn:aws:iam::414351767826:role/
    # unity-catalog-prod-UCMasterRole-14S5ZJVKOTYTL), not an AWS-owned
    # service principal. Building this role now would be non-functional.
    # See docs/VERIFICATION.md's "known gap" section.

    repl_role_name = f"{PREFIX}-s3-replication-role"
    repl_trust_policy = {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"Service": "s3.amazonaws.com"},
            "Action": "sts:AssumeRole",
        }],
    }
    repl_role_arn = create_role_if_not_exists(repl_role_name, repl_trust_policy)

    bronze_name = bucket_names["bronze"]
    gold_name = bucket_names["gold"]
    bronze_dr_name = f"{PREFIX}-bronze-dr"
    gold_dr_name = f"{PREFIX}-gold-dr"

    repl_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": ["s3:GetReplicationConfiguration", "s3:ListBucket"],
                "Resource": [f"arn:aws:s3:::{bronze_name}", f"arn:aws:s3:::{gold_name}"],
            },
            {
                "Effect": "Allow",
                "Action": [
                    "s3:GetObjectVersionForReplication",
                    "s3:GetObjectVersionAcl",
                    "s3:GetObjectVersionTagging",
                ],
                "Resource": [f"arn:aws:s3:::{bronze_name}/*", f"arn:aws:s3:::{gold_name}/*"],
            },
            {
                "Effect": "Allow",
                "Action": ["s3:ReplicateObject", "s3:ReplicateDelete", "s3:ReplicateTags"],
                "Resource": [f"arn:aws:s3:::{bronze_dr_name}/*", f"arn:aws:s3:::{gold_dr_name}/*"],
            },
        ],
    }
    put_inline_policy(repl_role_name, f"{PREFIX}-s3-replication-policy", repl_policy)

    record_resource(OUTPUTS, "s3_replication_role_arn", repl_role_arn)
    log_event(OUTPUTS, "step5", "complete", f"repl_role={repl_role_arn}")

    return {"s3_replication_role_arn": repl_role_arn}


# ─────────────────────────────────────────────────────────────────────────────
# STEP 6: DR BUCKETS + REPLICATION
# ─────────────────────────────────────────────────────────────────────────────

def create_dr_buckets_and_replication(bucket_names: dict, replication_role_arn: str) -> dict:
    print("\n--- Step 6: DR buckets + replication ---")
    log_event(OUTPUTS, "step6", "start", "creating DR buckets and replication config")

    dr_names = {}
    for key in ("bronze", "gold"):
        dr_name = f"{PREFIX}-{key}-dr"
        dr_names[f"{key}_dr"] = dr_name

        if bucket_exists(s3_dr, dr_name):
            print(f"  [skip] DR bucket already exists: {dr_name}")
        else:
            create_bucket_in_region(s3_dr, dr_name, DR_REGION)
            print(f"  [ok]   created bucket: {dr_name}")

        s3_dr.put_bucket_versioning(
            Bucket=dr_name,
            VersioningConfiguration={"Status": "Enabled"},
        )
        s3_dr.put_bucket_tagging(
            Bucket=dr_name,
            Tagging={"TagSet": aws_tag_list(COMMON_TAGS)},
        )
        record_resource(OUTPUTS, f"{key}_dr_bucket", dr_name)

    for key in ("bronze", "gold"):
        source_name = bucket_names[key]
        dr_name = dr_names[f"{key}_dr"]

        replication_config = {
            "Role": replication_role_arn,
            "Rules": [{
                "ID": f"{key}-to-dr",
                "Status": "Enabled",
                "Priority": 1,
                "Filter": {},
                "DeleteMarkerReplication": {"Status": "Enabled"},
                "Destination": {
                    "Bucket": f"arn:aws:s3:::{dr_name}",
                    "StorageClass": "STANDARD_IA",
                },
            }],
        }

        last_error = None
        for attempt in range(5):
            try:
                s3_primary.put_bucket_replication(
                    Bucket=source_name,
                    ReplicationConfiguration=replication_config,
                )
                print(f"  [ok]   replication configured: {key} -> {key}-dr")
                last_error = None
                break
            except ClientError as e:
                last_error = e
                wait = 2 ** attempt
                code = e.response["Error"]["Code"]
                print(f"  [retry] replication config attempt {attempt + 1} failed ({code}), waiting {wait}s...")
                time.sleep(wait)

        if last_error is not None:
            log_event(OUTPUTS, "step6", "error", f"replication config failed for {key}: {last_error}")
            raise last_error

    log_event(OUTPUTS, "step6", "complete", f"dr_buckets={dr_names}")
    return dr_names


# ─────────────────────────────────────────────────────────────────────────────
# STEP 7: CLOUDWATCH
# ─────────────────────────────────────────────────────────────────────────────

def create_cloudwatch_resources(stream_name: str) -> dict:
    print("\n--- Step 7: CloudWatch ---")
    log_event(OUTPUTS, "step7", "start", "creating dashboard and alarm")

    dashboard_name = f"{PREFIX}-ops"
    dashboard_body = {
        "widgets": [
            {
                "type": "metric",
                "properties": {
                    "title": "Kinesis — Iterator Age (Streaming Lag)",
                    "metrics": [["AWS/Kinesis", "GetRecords.IteratorAgeMilliseconds", "StreamName", stream_name]],
                    "region": PRIMARY_REGION,
                    "period": 60,
                    "stat": "Maximum",
                    "view": "timeSeries",
                },
            },
            {
                "type": "metric",
                "properties": {
                    "title": "Kinesis — Incoming Records/sec",
                    "metrics": [["AWS/Kinesis", "IncomingRecords", "StreamName", stream_name]],
                    "region": PRIMARY_REGION,
                    "period": 60,
                    "stat": "Sum",
                },
            },
            {
                "type": "metric",
                "properties": {
                    # Matches the deterministic name create_lambda_dlq() uses
                    # (f"{PREFIX}-kinesis-dlq-handler") -- referenced by name
                    # here rather than passed in, since this step historically
                    # runs before the Lambda step in __main__ and the metric
                    # simply reads as empty until the function exists.
                    "title": "Lambda DLQ Handler — Errors",
                    "metrics": [["AWS/Lambda", "Errors", "FunctionName", f"{PREFIX}-kinesis-dlq-handler"]],
                    "region": PRIMARY_REGION,
                    "period": 300,
                    "stat": "Sum",
                },
            },
        ],
    }
    cloudwatch.put_dashboard(
        DashboardName=dashboard_name,
        DashboardBody=json.dumps(dashboard_body),
    )
    print(f"  [ok]   dashboard created: {dashboard_name}")

    alarm_name = f"{PREFIX}-kinesis-lag-alarm"
    cloudwatch.put_metric_alarm(
        AlarmName=alarm_name,
        ComparisonOperator="GreaterThanThreshold",
        EvaluationPeriods=2,
        MetricName="GetRecords.IteratorAgeMilliseconds",
        Namespace="AWS/Kinesis",
        Period=60,
        Statistic="Maximum",
        Threshold=300000,
        AlarmDescription="Kinesis streaming lag exceeds 5 minutes",
        Dimensions=[{"Name": "StreamName", "Value": stream_name}],
        Tags=aws_tag_list(COMMON_TAGS),
    )
    print(f"  [ok]   alarm created: {alarm_name}")

    record_resource(OUTPUTS, "cloudwatch_dashboard_name", dashboard_name)
    record_resource(OUTPUTS, "cloudwatch_alarm_name", alarm_name)
    log_event(OUTPUTS, "step7", "complete", f"dashboard={dashboard_name} alarm={alarm_name}")

    return {"dashboard_name": dashboard_name, "alarm_name": alarm_name}


# ─────────────────────────────────────────────────────────────────────────────
# STEP 8: DATABRICKS CATALOGS / SCHEMAS / GROUPS
# ─────────────────────────────────────────────────────────────────────────────

CATALOG_SPECS = {
    "bronze": {"comment": "Raw ingestion layer — append-only, immutable", "schema": "market_data"},
    "silver": {"comment": "Cleaned, validated, conformed data", "schema": "market_data"},
    "gold": {"comment": "Star Schema — analytics and ML ready", "schema": "market_data"},
    "ml": {"comment": "ML models, features, experiments", "schema": "ml_artifacts"},
}

GROUP_NAMES = ["financial-intel-engineers", "financial-intel-analysts"]


def run_sql(warehouse_id: str, statement: str) -> None:
    response = w.statement_execution.execute_statement(
        statement=statement,
        warehouse_id=warehouse_id,
        wait_timeout="30s",
    )
    state = response.status.state.value
    if state != "SUCCEEDED":
        error = response.status.error
        raise RuntimeError(f"SQL statement failed ({state}): {statement!r} — {error}")


def create_databricks_objects() -> dict:
    print("\n--- Step 8: Databricks catalogs/schemas/groups ---")
    log_event(OUTPUTS, "step8", "start", "creating catalogs, schemas, groups via SQL (Default Storage, no grants)")

    warehouse = next(iter(w.warehouses.list()), None)
    if warehouse is None:
        raise RuntimeError("No SQL warehouse found in workspace — needed to run CREATE CATALOG/SCHEMA via Default Storage")
    warehouse_id = warehouse.id
    print(f"  using warehouse: {warehouse.name} ({warehouse_id})")

    # CREATE CATALOG uses the account's Default Storage automatically when no
    # MANAGED LOCATION is given — this is only reachable via SQL, not the
    # catalogs.create() REST/SDK call, since this metastore has no
    # metastore-level storage root configured (confirmed via account console).
    for name, spec in CATALOG_SPECS.items():
        run_sql(warehouse_id, f"CREATE CATALOG IF NOT EXISTS `{name}` COMMENT '{spec['comment']}'")
        print(f"  [ok]   catalog ensured: {name}")
        record_resource(OUTPUTS, f"catalog_{name}", name)

    for name, spec in CATALOG_SPECS.items():
        schema_name = spec["schema"]
        run_sql(warehouse_id, f"CREATE SCHEMA IF NOT EXISTS `{name}`.`{schema_name}`")
        print(f"  [ok]   schema ensured: {name}.{schema_name}")
        record_resource(OUTPUTS, f"schema_{name}_{schema_name}", f"{name}.{schema_name}")

    for group_name in GROUP_NAMES:
        try:
            existing = list(w.groups.list(filter=f'displayName eq "{group_name}"'))
            if existing:
                print(f"  [skip] group already exists: {group_name}")
                group_id = existing[0].id
            else:
                group = w.groups.create(display_name=group_name)
                print(f"  [ok]   group created: {group_name}")
                group_id = group.id
            record_resource(OUTPUTS, f"group_{group_name.replace('-', '_')}_id", group_id)
        except PermissionDenied:
            print(f"  [skip] no permission to manage groups (needs workspace admin, not just metastore admin): {group_name}")
            log_event(OUTPUTS, "step8", "skipped", f"group {group_name}: overarching-sp lacks workspace admin for SCIM Groups API")

    log_event(OUTPUTS, "step8", "complete", "catalogs/schemas/groups done")
    return OUTPUTS["resources"]


# ─────────────────────────────────────────────────────────────────────────────
# STEP 10: ALPACA API SECRET (Stage 1)
# ─────────────────────────────────────────────────────────────────────────────

def create_alpaca_secret() -> str:
    print("\n--- Step 10: Alpaca API secret ---")
    # Name must match the wildcard already granted to the databricks-boto3-runtime
    # IAM user (arn:...:secret:<prefix>/alpaca_api-*) — Secrets Manager appends a
    # random suffix automatically, which still satisfies that wildcard.
    secret_id = f"{PREFIX}/alpaca_api"

    if secret_exists(secret_id):
        print(f"  [skip] secret already exists: {secret_id}")
    else:
        secretsmanager.create_secret(
            Name=secret_id,
            SecretString=json.dumps({
                "api_key": require_env("ALPACA_API_KEY"),
                "api_secret": require_env("ALPACA_SECRET_KEY"),
            }),
            Tags=aws_tag_list(COMMON_TAGS),
        )
        print(f"  [ok]   created secret: {secret_id}")

    secret_arn = secretsmanager.describe_secret(SecretId=secret_id)["ARN"]
    record_resource(OUTPUTS, "alpaca_api_secret_arn", secret_arn)

    return secret_arn


# ─────────────────────────────────────────────────────────────────────────────
# STEP 11: LAMBDA DLQ HANDLER + KINESIS EVENT SOURCE MAPPING (Stage 1)
# ─────────────────────────────────────────────────────────────────────────────

DLQ_S3_PREFIX = "dlq"

# __DLQ_BUCKET__ is substituted with the real bronze bucket name before
# deployment — using .replace() rather than .format() so the handler's own
# f-strings don't need brace-escaping.
LAMBDA_DLQ_CODE_TEMPLATE = '''
import json
import boto3
import base64
from datetime import datetime, timezone

s3 = boto3.client("s3")
DLQ_BUCKET = "__DLQ_BUCKET__"
DLQ_PREFIX = "__DLQ_PREFIX__"


def handler(event, context):
    """Route malformed Kinesis market-data records to an S3 DLQ prefix."""
    failed_records = []

    for record in event["Records"]:
        try:
            payload = json.loads(base64.b64decode(record["kinesis"]["data"]))
            if not payload.get("symbol") or not payload.get("price"):
                raise ValueError(f"Missing required fields: {payload}")
        except Exception as e:
            failed_records.append({
                "record": record,
                "error": str(e),
                "failed_at": datetime.now(timezone.utc).isoformat(),
            })

    if failed_records:
        key = f"{DLQ_PREFIX}/{datetime.now(timezone.utc).strftime('%Y/%m/%d/%H')}/failed_{context.aws_request_id}.json"
        s3.put_object(
            Bucket=DLQ_BUCKET,
            Key=key,
            Body=json.dumps(failed_records),
        )

    return {"statusCode": 200, "failed_count": len(failed_records)}
'''


def lambda_function_exists(name: str) -> bool:
    try:
        lambda_client.get_function(FunctionName=name)
        return True
    except lambda_client.exceptions.ResourceNotFoundException:
        return False


def find_event_source_mapping(function_name: str, stream_arn: str):
    for esm in lambda_client.list_event_source_mappings(FunctionName=function_name)["EventSourceMappings"]:
        if esm["EventSourceArn"] == stream_arn:
            return esm["UUID"]
    return None


def create_lambda_dlq(bronze_bucket: str, stream_arn: str) -> dict:
    print("\n--- Step 11: Lambda DLQ handler ---")
    log_event(OUTPUTS, "step11", "start", "creating lambda-dlq-role, function, and Kinesis event source mapping")

    role_name = f"{PREFIX}-lambda-dlq-role"
    trust_policy = {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"Service": "lambda.amazonaws.com"},
            "Action": "sts:AssumeRole",
        }],
    }
    role_arn = create_role_if_not_exists(role_name, trust_policy)

    exec_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "kinesis:GetRecords", "kinesis:GetShardIterator",
                    "kinesis:DescribeStream", "kinesis:DescribeStreamSummary",
                    "kinesis:ListShards", "kinesis:ListStreams",
                ],
                "Resource": stream_arn,
            },
            {
                "Effect": "Allow",
                "Action": ["s3:PutObject"],
                "Resource": f"arn:aws:s3:::{bronze_bucket}/{DLQ_S3_PREFIX}/*",
            },
            {
                "Effect": "Allow",
                "Action": ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"],
                "Resource": "arn:aws:logs:*:*:*",
            },
        ],
    }
    put_inline_policy(role_name, f"{PREFIX}-lambda-dlq-policy", exec_policy)

    function_name = f"{PREFIX}-kinesis-dlq-handler"
    lambda_code = (
        LAMBDA_DLQ_CODE_TEMPLATE
        .replace("__DLQ_BUCKET__", bronze_bucket)
        .replace("__DLQ_PREFIX__", DLQ_S3_PREFIX)
    )
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("lambda_function.py", lambda_code)
    zip_bytes = buf.getvalue()

    if lambda_function_exists(function_name):
        lambda_client.update_function_code(FunctionName=function_name, ZipFile=zip_bytes)
        print(f"  [skip] function already exists (code refreshed): {function_name}")
    else:
        last_err = None
        for attempt in range(10):
            try:
                lambda_client.create_function(
                    FunctionName=function_name,
                    Runtime="python3.12",
                    Role=role_arn,
                    Handler="lambda_function.handler",
                    Code={"ZipFile": zip_bytes},
                    Timeout=30,
                    MemorySize=128,
                    Description="Routes malformed Kinesis market-data records to an S3 DLQ prefix",
                    # No Tags param: financial-intel-deploy lacks lambda:TagResource,
                    # and the PREFIX naming convention already identifies ownership.
                )
                print(f"  [ok]   created function: {function_name}")
                last_err = None
                break
            except ClientError as e:
                last_err = e
                code = e.response["Error"]["Code"]
                if code == "InvalidParameterValueException" and "cannot be assumed" in str(e).lower():
                    print(f"  [retry] role not yet assumable (attempt {attempt + 1}), waiting 5s...")
                    time.sleep(5)
                    continue
                raise
        if last_err is not None:
            raise last_err

    function_arn = lambda_client.get_function(FunctionName=function_name)["Configuration"]["FunctionArn"]
    record_resource(OUTPUTS, "lambda_dlq_function_arn", function_arn)
    record_resource(OUTPUTS, "lambda_dlq_role_arn", role_arn)
    record_resource(OUTPUTS, "dlq_s3_location", f"s3://{bronze_bucket}/{DLQ_S3_PREFIX}/")

    esm_uuid = find_event_source_mapping(function_name, stream_arn)
    if esm_uuid:
        print(f"  [skip] event source mapping already exists: {esm_uuid}")
    else:
        last_err = None
        for attempt in range(10):
            try:
                resp = lambda_client.create_event_source_mapping(
                    EventSourceArn=stream_arn,
                    FunctionName=function_name,
                    StartingPosition="LATEST",
                    BatchSize=10,
                )
                esm_uuid = resp["UUID"]
                print(f"  [ok]   created event source mapping: {esm_uuid}")
                last_err = None
                break
            except ClientError as e:
                last_err = e
                if e.response["Error"]["Code"] == "InvalidParameterValueException":
                    print(f"  [retry] function/role not yet ready for ESM (attempt {attempt + 1}), waiting 5s...")
                    time.sleep(5)
                    continue
                raise
        if last_err is not None:
            raise last_err

    record_resource(OUTPUTS, "lambda_dlq_event_source_mapping_uuid", esm_uuid)
    log_event(OUTPUTS, "step11", "complete", f"function={function_arn} esm={esm_uuid}")

    return {"function_arn": function_arn, "role_arn": role_arn, "esm_uuid": esm_uuid}


# ─────────────────────────────────────────────────────────────────────────────
# STEP 9: WRAP-UP
# ─────────────────────────────────────────────────────────────────────────────

def wrap_up() -> None:
    print("\n--- Step 9: Wrap-up ---")
    log_event(OUTPUTS, "step9", "complete", "deployment finished")
    print(json.dumps(OUTPUTS["resources"], indent=2))


if __name__ == "__main__":
    print("Config loaded successfully. AWS + Databricks clients initialized.")
    print(f"Primary region: {PRIMARY_REGION} | DR region: {DR_REGION}")
    print(f"Databricks host: {os.environ['DATABRICKS_HOST']}")

    buckets = create_medallion_buckets()
    print("\nBucket names:", json.dumps(buckets, indent=2))

    stream_name = create_kinesis_stream()
    print("\nKinesis stream:", stream_name)

    secret_arn = create_sec_edgar_secret()
    print("\nSEC EDGAR secret ARN:", secret_arn)

    role_arns = create_iam_roles(buckets)
    print("\nIAM roles:", json.dumps(role_arns, indent=2))

    dr_buckets = create_dr_buckets_and_replication(buckets, role_arns["s3_replication_role_arn"])
    print("\nDR buckets:", json.dumps(dr_buckets, indent=2))

    cw_resources = create_cloudwatch_resources(stream_name)
    print("\nCloudWatch:", json.dumps(cw_resources, indent=2))

    alpaca_secret_arn = create_alpaca_secret()
    print("\nAlpaca API secret ARN:", alpaca_secret_arn)

    stream_arn = OUTPUTS["resources"]["kinesis_stream_arn"]
    lambda_dlq = create_lambda_dlq(buckets["bronze"], stream_arn)
    print("\nLambda DLQ:", json.dumps(lambda_dlq, indent=2))

    try:
        databricks_objects = create_databricks_objects()
        print("\nDatabricks objects recorded in outputs.json")
    except Exception as e:
        # Don't let a broken overarching-sp OAuth credential (unrelated to
        # this run's AWS work above) block everything else — catalogs/schemas
        # were already confirmed manually during Phase 0 anyway.
        print(f"\n  [warn] Step 8 (Databricks catalogs/schemas/groups) failed, continuing: {e}")
        log_event(OUTPUTS, "step8", "error", str(e))

    wrap_up()
