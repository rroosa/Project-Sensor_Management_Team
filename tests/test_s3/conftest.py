import boto3
import os
import pytest
import sys
from moto import mock_aws
sys.path.append("../../")
from ServiceS3.S3_botoObj import S3BotoObj

@pytest.fixture
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"


@pytest.fixture
def s3_client(aws_credentials):
    with mock_aws():
        conn = boto3.client("s3", region_name="us-east-1")
        yield conn



@pytest.fixture
def sts_client(aws_credentials):
    with mock_aws():
        conn_sts = boto3.client("sts",region_name="us-east-1")
        yield conn_sts

@pytest.fixture
def id_account_aws():
    return "123456789"

@pytest.fixture
def group_name():
    return "SensorTeamTest"

@pytest.fixture
def assume_role_arn(id_account_aws,group_name):
    assume_role_arn = f"arn:aws:iam::{id_account_aws}:role/role_assumeRoleUsers-{group_name}"
    return assume_role_arn

@pytest.fixture
def session_name():
    return "AssumeRoleSession_Test"


@pytest.fixture
def bucket_name():
    return "sensors-data-sheet-test"

@pytest.fixture
def tmp_folder_name():
    return "folder_tmp"

@pytest.fixture
def bucket_mock(s3_client, bucket_name):
    s3_client.create_bucket(Bucket=bucket_name)
    yield

@pytest.fixture
def root_path():
    ROOT_DIR =os.path.dirname(
        os.path.abspath(__file__))
    return ROOT_DIR

    
@pytest.fixture
def s3_botoObj(s3_client,sts_client,id_account_aws,assume_role_arn,session_name,root_path,tmp_folder_name):

    s3_botoObj = S3BotoObj.getInstance()
    s3_botoObj.setRootPath(root_path)
    s3_botoObj.set_idAccount(id_account_aws)
    s3_botoObj.setAssume_Role_Arn(assume_role_arn)
    s3_botoObj.setRoleSessionName(session_name)
    s3_botoObj.setUploadFolder(tmp_folder_name)
    s3_botoObj.setDownloadFolder(tmp_folder_name)
    s3_botoObj.sessionWithRefresh()
    return s3_botoObj
