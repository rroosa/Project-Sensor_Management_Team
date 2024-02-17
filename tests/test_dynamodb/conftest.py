import sys
#print(sys.executable)
#print ("\n".join(sys.path))
import boto3
import os
import pytest

import json
from decimal import Decimal
from tempfile import NamedTemporaryFile
from moto import mock_aws
from moto.dynamodb.models import Table
sys.path.append("../../")
from ServiceDB.dynamo_botoObj import DynamoBotoObj



@pytest.fixture
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"


@pytest.fixture
def db_client(aws_credentials):
    with mock_aws():
        conn = boto3.client("dynamodb", region_name="us-west-2")
        yield conn


@pytest.fixture
def db_resource(aws_credentials):
    with mock_aws():
        conn_r = boto3.resource("dynamodb", region_name="us-west-2")
        yield conn_r

@pytest.fixture
def sts_client(aws_credentials):
    with mock_aws():
        conn_sts = boto3.client("sts",region_name="us-west-2")
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
def tmp_folder_name():
    return "folder_tmp"

@pytest.fixture
def root_path():
    ROOT_DIR =os.path.dirname(
        os.path.abspath(__file__))
    return ROOT_DIR

@pytest.fixture
def table_schema():
    return Table(
        "Model_Device_1",
        region = 'us-west-2',
        schema = [
            {"KeyType": "HASH", "AttributeName": "id_device"},
            {"KeyType": "RANGE", "AttributeName": "time_id"}
        ],
        attr = [
            {"AttributeType": "S", "AttributeName": "id_device"},
            {"AttributeType": "S", "AttributeName": "time_id"}
        ],
    )
@pytest.fixture
def name_table():
    yield "Model_Device_1"

@pytest.fixture
def list_KeySchema():
    keySchema = [
            {"AttributeName":"id_device", "KeyType":"HASH"},
            {"AttributeName":"time_id", "KeyType": "RANGE"}
        ]
    return keySchema

@pytest.fixture
def list_AttributeDef():
    attrDef = [
            {"AttributeName": "id_device", "AttributeType": "S"},
            {"AttributeName": "time_id", "AttributeType": "S"}
        ]
    return attrDef

@pytest.fixture
def throughput():
    obj = {
            'ReadCapacityUnits':5,
            'WriteCapacityUnits':5
        }
    return obj

@pytest.fixture
def table_mock(db_resource, name_table,list_KeySchema,list_AttributeDef,throughput):
    with mock_aws():
        table = db_resource.create_table(
            TableName = name_table,
            KeySchema = list_KeySchema,
            AttributeDefinitions = list_AttributeDef,
            ProvisionedThroughput = throughput
        )
        table.meta.client.get_waiter('table_exists').wait(TableName= name_table)
        yield table

@pytest.fixture
def data_json():
    json_data = [{"time_id":"2021-09-30T21:08:02","id_device":"D_HomeQ-01","ping_ms":17.28,"temperature_c":25.0,"humidity_p":35.0}]

    return json.dumps(json_data)

@pytest.fixture
def data_device_two():
    data = [
        {"time_id":"2021-09-30T21:08:02","id_device":"D_HomeQ-01","ping_ms":17.28,"temperature_c":25.0,"humidity_p":35.0},
        {"time_id":"2021-09-30T21:09:02","id_device":"D_HomeQ-01","ping_ms":17.73,"temperature_c":23.0,"humidity_p":40.0},
        {"time_id":"2021-09-30T21:10:01","id_device":"D_HomeQ-01","ping_ms":18.59,"temperature_c":22.0,"humidity_p":41.0},
        {"time_id":"2021-09-30T21:12:02","id_device":"D_HomeQ-01","ping_ms":16.73,"temperature_c":22.0,"humidity_p":42.0},
        {"time_id":"2021-09-30T21:13:02","id_device":"D_HomeQ-01","ping_ms":18.12,"temperature_c":27.0,"humidity_p":42.0},
        {"time_id":"2021-09-30T21:14:01","id_device":"D_HomeQ-01","ping_ms":18.21,"temperature_c":22.0,"humidity_p":43.0},
        {"time_id":"2021-09-30T21:15:01","id_device":"D_HomeQ-01","ping_ms":17.92,"temperature_c":22.0,"humidity_p":43.0},
        {"time_id":"2021-09-30T21:16:02","id_device":"D_HomeQ-01","ping_ms":17.2,"temperature_c":22.0,"humidity_p":43.0},
        {"time_id":"2021-09-30T21:17:02","id_device":"D_HomeQ-01","ping_ms":18.16,"temperature_c":22.0,"humidity_p":43.0},
        {"time_id":"2021-09-30T21:18:02","id_device":"D_HomeQ-01","ping_ms":21.35,"temperature_c":22.0,"humidity_p":42.0},
        {"time_id":"2021-09-30T21:08:02","id_device":"D_HomeQ-02","ping_ms":17.28,"temperature_c":45.0,"humidity_p":22.0},
        {"time_id":"2021-09-30T21:09:02","id_device":"D_HomeQ-02","ping_ms":17.73,"temperature_c":43.0,"humidity_p":23.0},
        {"time_id":"2021-09-30T21:10:01","id_device":"D_HomeQ-02","ping_ms":18.59,"temperature_c":42.0,"humidity_p":21.0},
        {"time_id":"2021-09-30T21:12:02","id_device":"D_HomeQ-02","ping_ms":16.73,"temperature_c":42.0,"humidity_p":13.0},
        {"time_id":"2021-09-30T21:13:02","id_device":"D_HomeQ-02","ping_ms":18.12,"temperature_c":42.0,"humidity_p":13.0}

    ]
    #return data
    return json.dumps(data)

@pytest.fixture
def table_mock_with_data(data_device_two, db_resource, name_table,table_mock):
    with NamedTemporaryFile(delete=True, suffix=".json") as tmp:
        with open(tmp.name, "w", encoding="UTF-8") as f:
            f.write(data_device_two)

        # lettura dal file per effettuare la scrittura in batch nella tabella fittizia
        with open(tmp.name) as file:
            dataset_item = json.load( file, parse_float=Decimal)
            with db_resource.Table(name_table).batch_writer() as writer:
                for item in dataset_item:
                    writer.put_item(Item= item)

    yield db_resource.Table(name_table)



@pytest.fixture
def data_device_2():
    data = [
        {"time_id":"2021-09-30T21:08:02","id_device":"D_HomeQ-02","ping_ms":17.28,"temperature_c":45.0,"humidity_p":22.0},
        {"time_id":"2021-09-30T21:09:02","id_device":"D_HomeQ-02","ping_ms":17.73,"temperature_c":43.0,"humidity_p":23.0},
        {"time_id":"2021-09-30T21:10:01","id_device":"D_HomeQ-02","ping_ms":18.59,"temperature_c":42.0,"humidity_p":21.0},
        {"time_id":"2021-09-30T21:12:02","id_device":"D_HomeQ-02","ping_ms":16.73,"temperature_c":42.0,"humidity_p":13.0},
        {"time_id":"2021-09-30T21:13:02","id_device":"D_HomeQ-02","ping_ms":18.12,"temperature_c":42.0,"humidity_p":13.0}
    ]
    #return data
    return json.dumps(data)

@pytest.fixture
def json_file_template(tmpdir,tmp_folder_name, name_table):
    json_data = {   
        "sensorDevice_model":name_table,
        "keys": [
            {"field": "time_id"  ,"type":"sort"},
            {"field": "id_device", "type":"partition"}
            
        ],

        "keys_measures":[
            {"field": "time_id" ,"type":"string"},
            {"field": "id_device", "type":"string"} 
        ],
        
        "other_measures":[
            {"field": "ping_ms", "type": "number"},
            {"field": "temperature_c", "type": "number"},
            {"field": "humidity_p", "type": "number"}
        ]
    }
    json_str = json.dumps(json_data)

    tmp_file_path = tmpdir.mkdir(tmp_folder_name)/"file_template.json"
    tmp_file_path.write(json_str)
    
    yield tmp_file_path

@pytest.fixture
def json_file_template_2(tmpdir,tmp_folder_name, name_table):
    json_data = {   
        "sensorDevice_model":name_table,
        "keys": [
            {"field": "time_id"  ,"type":"sort"},
            {"field": "id_device", "type":"partition"}
            
        ],

        "keys_measures":[
            {"field": "non_match" ,"type":"string"},
            {"field": "id_device", "type":"string"} 
        ],
        
        "other_measures":[
            {"field": "ping_ms", "type": "number"},
            {"field": "temperature_c", "type": "number"},
            {"field": "humidity_p", "type": "number"}
        ]
    }
    json_str = json.dumps(json_data)

    tmp_file_path = tmpdir.mkdir(tmp_folder_name)/"file_template.json"
    tmp_file_path.write(json_str)
    
    yield tmp_file_path

@pytest.fixture
def json_file_template_3(tmpdir,tmp_folder_name, name_table):
    json_data = {   
        "sensorDevice_model":name_table,
        "keys": [
            {"field": "time_id"  ,"type":"sort"},
            {"field": "id_device", "type":"error_type_key"}
            
        ],

        "keys_measures":[
            {"field": "time_id" ,"type":"string"},
            {"field": "id_device", "type":"string"} 
        ],
        
        "other_measures":[
            {"field": "ping_ms", "type": "number"},
            {"field": "temperature_c", "type": "number"},
            {"field": "humidity_p", "type": "number"}
        ]
    }
    json_str = json.dumps(json_data)

    tmp_file_path = tmpdir.mkdir(tmp_folder_name)/"file_template.json"
    tmp_file_path.write(json_str)
    
    yield tmp_file_path

@pytest.fixture
def json_file_template_4(tmpdir,tmp_folder_name, name_table):
    json_data = {   
        "attr_NotPresent:sensorDevice_model":name_table,
        "keys": [
            {"field": "time_id"  ,"type":"sort"},
            {"field": "id_device", "type":"error_type_key"}
            
        ],

        "keys_measures":[
            {"field": "time_id" ,"type":"string"},
            {"field": "id_device", "type":"string"} 
        ],
        
        "other_measures":[
            {"field": "ping_ms", "type": "number"},
            {"field": "temperature_c", "type": "number"},
            {"field": "humidity_p", "type": "number"}
        ]
    }
    json_str = json.dumps(json_data)

    tmp_file_path = tmpdir.mkdir(tmp_folder_name)/"file_template.json"
    tmp_file_path.write(json_str)
    
    yield tmp_file_path

@pytest.fixture
def json_file_template_5(tmpdir,tmp_folder_name, name_table):
    json_data = {   
        "sensorDevice_model":name_table,
        "keys": [
            {"field": "time_id"  ,"type":"sort"},
            {"field": "id_device", "type":"error_type_key"}
            
        ],

        "keys_measures":[
            {"field": "time_id" ,"type":"string"}
        ],
        
        "other_measures":[
            {"field": "ping_ms", "type": "number"},
            {"field": "temperature_c", "type": "number"},
            {"field": "humidity_p", "type": "number"}
        ]
    }
    json_str = json.dumps(json_data)

    tmp_file_path = tmpdir.mkdir(tmp_folder_name)/"file_template.json"
    tmp_file_path.write(json_str)
    
    yield tmp_file_path
@pytest.fixture
def json_file_template_6(tmpdir,tmp_folder_name, name_table):
    json_data = {   
        "sensorDevice_model":name_table,
        "keys": [
            {"field": "time_id"  ,"type":"sort"},
            {"field": "id_device", "type":"partition"}
        ]
    }
    json_str = json.dumps(json_data)

    tmp_file_path = tmpdir.mkdir(tmp_folder_name)/"file_template.json"
    tmp_file_path.write(json_str)
    
    yield tmp_file_path


@pytest.fixture
def db_botoObj(db_client,db_resource,sts_client,id_account_aws,assume_role_arn,session_name,root_path,tmp_folder_name,table_mock):

    db_botoObj = DynamoBotoObj.getInstance()
    db_botoObj.setRootPath(root_path)
    db_botoObj.set_IdAccount(id_account_aws)
    db_botoObj.setAssume_Role_Arn(assume_role_arn)
    db_botoObj.set_RoleSessionName(session_name)
    db_botoObj.setTemplateFolder(tmp_folder_name)
    db_botoObj.setTemplateFolder(tmp_folder_name)
    db_botoObj.sessionWithRefresh()
    return db_botoObj


