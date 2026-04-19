import socket
import subprocess
import tempfile
import time
import urllib.request
import shutil
import pytest
import boto3
import os
import signal


def _find_free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("localhost", 0))
        return s.getsockname()[1]


DEVCLOUD_PORT = int(os.environ.get("DEVCLOUD_PORT", "0"))
if DEVCLOUD_PORT == 0:
    DEVCLOUD_PORT = _find_free_port()
DEVCLOUD_URL = os.environ.get("DEVCLOUD_URL", f"http://localhost:{DEVCLOUD_PORT}")


def _build_devcloud_cmd(project_root, bin_path):
    """Build the devcloud command, conditionally adding -config if devcloud.yaml exists."""
    config_path = os.path.join(project_root, "devcloud.yaml")

    if bin_path:
        cmd = [bin_path]
    else:
        cmd = ["go", "run", "./cmd/devcloud"]

    if os.path.isfile(config_path):
        cmd.extend(["-config", "devcloud.yaml"])

    return cmd


def _start_server_error(cmd, project_root, env):
    """Re-run server with PIPE to capture stderr, then raise with diagnostic info."""
    debug_proc = subprocess.Popen(
        cmd,
        cwd=project_root,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    debug_timeout = 5
    try:
        _wait_for_server(DEVCLOUD_URL, timeout=debug_timeout)
    except RuntimeError:
        pass
    debug_proc.kill()
    debug_proc.wait()
    stderr = debug_proc.stderr.read().decode(errors="replace")
    raise RuntimeError(
        f"devcloud server did not start within {debug_timeout}s (diagnostic re-run).\n"
        f"command: {' '.join(cmd)}\n"
        f"stderr:\n{stderr}"
    ) from None


def _wait_for_server(url, timeout=30, interval=0.5):
    """Poll server until it responds or timeout."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            urllib.request.urlopen(url, timeout=2)
            return True
        except Exception:
            time.sleep(interval)
    raise RuntimeError(f"devcloud server did not start within {timeout}s")


@pytest.fixture(scope="session")
def devcloud_server():
    """Start devcloud server for the test session.

    Modes:
      - DEVCLOUD_EXTERNAL=1: connect to an already-running server (e.g. Docker)
      - DEVCLOUD_BIN=/path: run a pre-built binary
      - (default): run via ``go run``
    """
    if os.environ.get("DEVCLOUD_EXTERNAL"):
        _wait_for_server(DEVCLOUD_URL)
        yield None
        return

    bin_path = os.environ.get("DEVCLOUD_BIN")
    project_root = os.path.join(os.path.dirname(__file__), "../..")

    # Use a fresh temporary directory for each test session so runs never
    # collide with stale data.  The directory is cleaned up after the session.
    data_dir = tempfile.mkdtemp(prefix="devcloud-test-")

    cmd = _build_devcloud_cmd(project_root, bin_path)

    env = os.environ.copy()
    env["CGO_ENABLED"] = "1"
    env["DEVCLOUD_DATA_DIR"] = data_dir
    env["DEVCLOUD_PORT"] = str(DEVCLOUD_PORT)

    proc = subprocess.Popen(
        cmd,
        cwd=project_root,
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        _wait_for_server(DEVCLOUD_URL)
    except RuntimeError:
        proc.kill()
        proc.wait()
        _start_server_error(cmd, project_root, env)
    yield proc
    proc.send_signal(signal.SIGINT)
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
    shutil.rmtree(data_dir, ignore_errors=True)


def _make_client(service_name):
    """Create a boto3 client pointing at devcloud."""
    from botocore.config import Config

    config_kwargs = {
        "connect_timeout": 5,
        "read_timeout": 10,
        "retries": {"max_attempts": 0},
    }
    # Newer botocore versions support inject_host_prefix — disable so services
    # like MWAA (api.) and IoT data plane don't prepend subdomains that can't
    # resolve against localhost.
    try:
        config = Config(**config_kwargs, inject_host_prefix=False)
    except TypeError:
        config = Config(**config_kwargs)
    return boto3.client(
        service_name,
        endpoint_url=DEVCLOUD_URL,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
        config=config,
    )


# --- Existing service fixtures ---


@pytest.fixture
def s3_client(devcloud_server):
    return _make_client("s3")


@pytest.fixture
def sqs_client(devcloud_server):
    return _make_client("sqs")


@pytest.fixture
def dynamodb_client(devcloud_server):
    return _make_client("dynamodb")


@pytest.fixture
def iam_client(devcloud_server):
    return _make_client("iam")


@pytest.fixture
def sts_client(devcloud_server):
    return _make_client("sts")


@pytest.fixture
def lambda_client(devcloud_server):
    return _make_client("lambda")


@pytest.fixture
def sns_client(devcloud_server):
    return _make_client("sns")


@pytest.fixture
def kms_client(devcloud_server):
    return _make_client("kms")


@pytest.fixture
def secretsmanager_client(devcloud_server):
    return _make_client("secretsmanager")


@pytest.fixture
def ssm_client(devcloud_server):
    return _make_client("ssm")


@pytest.fixture
def logs_client(devcloud_server):
    return _make_client("logs")


@pytest.fixture
def cloudwatch_client(devcloud_server):
    return _make_client("cloudwatch")


@pytest.fixture
def events_client(devcloud_server):
    return _make_client("events")


@pytest.fixture
def ec2_client(devcloud_server):
    return _make_client("ec2")


@pytest.fixture
def ecs_client(devcloud_server):
    return _make_client("ecs")


@pytest.fixture
def ecr_client(devcloud_server):
    return _make_client("ecr")


@pytest.fixture
def route53_client(devcloud_server):
    return _make_client("route53")


@pytest.fixture
def acm_client(devcloud_server):
    return _make_client("acm")


# --- New service fixtures ---


@pytest.fixture
def rds_client(devcloud_server):
    return _make_client("rds")


@pytest.fixture
def cloudformation_client(devcloud_server):
    return _make_client("cloudformation")


@pytest.fixture
def elasticache_client(devcloud_server):
    return _make_client("elasticache")


@pytest.fixture
def elbv2_client(devcloud_server):
    return _make_client("elbv2")


@pytest.fixture
def redshift_client(devcloud_server):
    return _make_client("redshift")


@pytest.fixture
def ses_client(devcloud_server):
    return _make_client("ses")


@pytest.fixture
def autoscaling_client(devcloud_server):
    return _make_client("autoscaling")


@pytest.fixture
def docdb_client(devcloud_server):
    return _make_client("docdb")


@pytest.fixture
def neptune_client(devcloud_server):
    return _make_client("neptune")


@pytest.fixture
def elasticbeanstalk_client(devcloud_server):
    return _make_client("elasticbeanstalk")


@pytest.fixture
def cloudsearch_client(devcloud_server):
    return _make_client("cloudsearch")


@pytest.fixture
def apigatewayv2_client(devcloud_server):
    return _make_client("apigatewayv2")


@pytest.fixture
def opensearch_client(devcloud_server):
    return _make_client("opensearch")


@pytest.fixture
def waf_client(devcloud_server):
    return _make_client("waf")


@pytest.fixture
def backup_client(devcloud_server):
    return _make_client("backup")


@pytest.fixture
def glue_client(devcloud_server):
    return _make_client("glue")


@pytest.fixture
def sagemaker_client(devcloud_server):
    return _make_client("sagemaker")


@pytest.fixture
def iot_client(devcloud_server):
    return _make_client("iot")


@pytest.fixture
def route53resolver_client(devcloud_server):
    return _make_client("route53resolver")


@pytest.fixture
def sesv2_client(devcloud_server):
    return _make_client("sesv2")


@pytest.fixture
def pinpoint_client(devcloud_server):
    return _make_client("pinpoint")


# --- Extended service fixtures ---


@pytest.fixture
def acmpca_client(devcloud_server):
    return _make_client("acm-pca")


@pytest.fixture
def amplify_client(devcloud_server):
    return _make_client("amplify")


@pytest.fixture
def appconfig_client(devcloud_server):
    return _make_client("appconfig")


@pytest.fixture
def applicationautoscaling_client(devcloud_server):
    return _make_client("application-autoscaling")


@pytest.fixture
def appsync_client(devcloud_server):
    return _make_client("appsync")


@pytest.fixture
def athena_client(devcloud_server):
    return _make_client("athena")


@pytest.fixture
def batch_client(devcloud_server):
    return _make_client("batch")


@pytest.fixture
def bedrock_client(devcloud_server):
    return _make_client("bedrock")


@pytest.fixture
def cloudfront_client(devcloud_server):
    return _make_client("cloudfront")


@pytest.fixture
def cloudtrail_client(devcloud_server):
    return _make_client("cloudtrail")


@pytest.fixture
def codeartifact_client(devcloud_server):
    return _make_client("codeartifact")


@pytest.fixture
def codebuild_client(devcloud_server):
    return _make_client("codebuild")


@pytest.fixture
def codecommit_client(devcloud_server):
    return _make_client("codecommit")


@pytest.fixture
def codedeploy_client(devcloud_server):
    return _make_client("codedeploy")


@pytest.fixture
def codepipeline_client(devcloud_server):
    return _make_client("codepipeline")


@pytest.fixture
def cognitoidentity_client(devcloud_server):
    return _make_client("cognito-identity")


@pytest.fixture
def cognitoidp_client(devcloud_server):
    return _make_client("cognito-idp")


@pytest.fixture
def configservice_client(devcloud_server):
    return _make_client("config")


@pytest.fixture
def costexplorer_client(devcloud_server):
    return _make_client("ce")


@pytest.fixture
def dynamodbstreams_client(devcloud_server):
    return _make_client("dynamodbstreams")


@pytest.fixture
def ebs_client(devcloud_server):
    return _make_client("ebs")


@pytest.fixture
def efs_client(devcloud_server):
    return _make_client("efs")


@pytest.fixture
def eks_client(devcloud_server):
    return _make_client("eks")


@pytest.fixture
def elasticsearch_client(devcloud_server):
    return _make_client("es")


@pytest.fixture
def emr_client(devcloud_server):
    return _make_client("emr")


@pytest.fixture
def firehose_client(devcloud_server):
    return _make_client("firehose")


@pytest.fixture
def fis_client(devcloud_server):
    return _make_client("fis")


@pytest.fixture
def glacier_client(devcloud_server):
    return _make_client("glacier")


@pytest.fixture
def iotdata_client(devcloud_server):
    return _make_client("iot-data")


@pytest.fixture
def iotwireless_client(devcloud_server):
    return _make_client("iotwireless")


@pytest.fixture
def kafka_client(devcloud_server):
    return _make_client("kafka")


@pytest.fixture
def kinesis_client(devcloud_server):
    return _make_client("kinesis")


@pytest.fixture
def kinesisanalyticsv2_client(devcloud_server):
    return _make_client("kinesisanalyticsv2")


@pytest.fixture
def lakeformation_client(devcloud_server):
    return _make_client("lakeformation")


@pytest.fixture
def managedblockchain_client(devcloud_server):
    return _make_client("managedblockchain")


@pytest.fixture
def memorydb_client(devcloud_server):
    return _make_client("memorydb")


@pytest.fixture
def mq_client(devcloud_server):
    return _make_client("mq")


@pytest.fixture
def mwaa_client(devcloud_server):
    return _make_client("mwaa")


@pytest.fixture
def organizations_client(devcloud_server):
    return _make_client("organizations")


@pytest.fixture
def ram_client(devcloud_server):
    return _make_client("ram")


@pytest.fixture
def resourcegroups_client(devcloud_server):
    return _make_client("resource-groups")


@pytest.fixture
def resourcegroupstagging_client(devcloud_server):
    return _make_client("resourcegroupstaggingapi")


@pytest.fixture
def servicediscovery_client(devcloud_server):
    return _make_client("servicediscovery")


@pytest.fixture
def sfn_client(devcloud_server):
    return _make_client("stepfunctions")


@pytest.fixture
def shield_client(devcloud_server):
    return _make_client("shield")


@pytest.fixture
def ssoadmin_client(devcloud_server):
    return _make_client("sso-admin")


@pytest.fixture
def support_client(devcloud_server):
    return _make_client("support")


@pytest.fixture
def swf_client(devcloud_server):
    return _make_client("swf")


@pytest.fixture
def textract_client(devcloud_server):
    return _make_client("textract")


@pytest.fixture
def timestreamwrite_client(devcloud_server):
    return _make_client("timestream-write")


@pytest.fixture
def transcribe_client(devcloud_server):
    return _make_client("transcribe")


@pytest.fixture
def transfer_client(devcloud_server):
    return _make_client("transfer")


@pytest.fixture
def wafv2_client(devcloud_server):
    return _make_client("wafv2")


@pytest.fixture
def xray_client(devcloud_server):
    return _make_client("xray")


@pytest.fixture
def pipes_client(devcloud_server):
    return _make_client("pipes")


@pytest.fixture
def cloudcontrol_client(devcloud_server):
    return _make_client("cloudcontrol")


@pytest.fixture
def account_client(devcloud_server):
    return _make_client("account")


@pytest.fixture
def mediaconvert_client(devcloud_server):
    return _make_client("mediaconvert")


@pytest.fixture
def s3tables_client(devcloud_server):
    return _make_client("s3tables")


@pytest.fixture
def scheduler_client(devcloud_server):
    return _make_client("scheduler")


@pytest.fixture
def identitystore_client(devcloud_server):
    return _make_client("identitystore")


@pytest.fixture
def serverlessrepo_client(devcloud_server):
    return _make_client("serverlessrepo")
