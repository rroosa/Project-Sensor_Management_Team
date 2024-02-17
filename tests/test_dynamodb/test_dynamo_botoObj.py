import pytest
import os
import sys
import json
from moto import mock_aws
from decimal import Decimal
from tempfile import NamedTemporaryFile
from botocore.exceptions import ClientError
sys.path.append("../../")
from ServiceDB.dynamo_botoObj import DynamoBotoObj
from contextlib import contextmanager
import pandas as pd



def test_create_table_default(db_resource,db_client,table_mock,name_table):
	res = db_client.describe_table( TableName = name_table)
	res2 = db_client.list_tables()

	assert res['Table']['TableName'] == name_table
	assert res2['TableNames'] == [name_table]
	print(res2)
	assert res['Table']["ItemCount"] == 0

def test_write_data_table_default(db_resource,db_client,table_mock,tmpdir,name_table,data_json,tmp_folder_name):
	tmp_file_path = tmpdir.mkdir(tmp_folder_name)/"data.json"
	tmp_file_path.write(data_json)

	with open(tmp_file_path) as file:
			dataset_item = json.load( file, parse_float=Decimal)
	
	with db_resource.Table(name_table).batch_writer() as writer:
		for item in dataset_item:
			writer.put_item(Item = item)

	res = db_client.describe_table( TableName = name_table)
	assert res['Table']["ItemCount"] == 1





def test_createTable( db_botoObj,db_resource,list_KeySchema,db_client, throughput,list_AttributeDef,table_mock):
	print(list_KeySchema)
	n_table = "Model_Device_HP"
	db_botoObj.list_KeySchema = list_KeySchema
	db_botoObj.list_AttributeDefinitions = list_AttributeDef

	message, code = db_botoObj.createTable(n_table)
	db_resource.create_table(
        TableName = "NewTable",
        KeySchema = list_KeySchema,
        AttributeDefinitions = list_AttributeDef,
        ProvisionedThroughput = throughput
    )
	#sassert code == 100
	res2 = db_client.list_tables()
	print(res2)




def test_createTable_IncorrectName(db_resource,db_client, db_botoObj,table_mock, name_table,list_KeySchema, list_AttributeDef):
	print(list_KeySchema)
	n_table = "Mo"
	db_botoObj.list_KeySchema = list_KeySchema
	db_botoObj.list_AttributeDefinitions = list_AttributeDef


	message, code = db_botoObj.createTable(n_table)
	assert code == 606
	assert message == f"The parameters you provider are incorrect: Parameter validation failed:\n"+"Invalid length for parameter TableName, value: 2, valid min length: 3"
	res = db_client.describe_table( TableName = name_table)
	assert res['Table']['TableName'] == name_table



def test_writeData(db_resource,db_client,table_mock,name_table,db_botoObj,tmpdir,root_path,tmp_folder_name,data_json):
	# scrivi i dati da file json nella tabella di default

	tmp_file_path = tmpdir.mkdir(tmp_folder_name)/"data.json"
	tmp_file_path.write(data_json)


	mess, code = db_botoObj.writeData(name_table,tmp_file_path,table_mock)
	assert code == 200

	res = db_client.describe_table( TableName = name_table)
	assert res['Table']["ItemCount"] == 1

def test_writeData_ErrorParamValidation(db_resource,db_client,table_mock,name_table,db_botoObj,tmpdir,root_path,tmp_folder_name,data_json):
	data_json_errorencode = {"time_id":"2021-09-30T21:08:02","id_device":"D_HomeQ-01","ping_ms":17.28,"temperature_c":25.0,"humidity_p":35.0}
	tmp_file_path = tmpdir.mkdir(tmp_folder_name)/"data.json"
	tmp_file_path.write(json.dumps(data_json_errorencode))
	mess, code = db_botoObj.writeData(name_table,tmp_file_path,table_mock)
	assert code == 808
	assert mess == f"Error Param Validation, it is requests the list of elements"

def test_writeData_ParamValidationError(db_resource,db_client,table_mock,name_table,db_botoObj,tmpdir,root_path,tmp_folder_name,data_json):
	text_content = 	f"time_id2021-09-30T210802,id_device:D_HomeQ-01,ping_ms:17.28,temperature_c:25.0,humidity_p:35.0"

	tmp_file_path = tmpdir.mkdir(tmp_folder_name)/"data.txt"
	tmp_file_path.write(json.dumps(text_content))
	mess, code = db_botoObj.writeData(name_table,tmp_file_path,table_mock)

	assert code == 808
	assert mess == f"Error Param Validation, it is requests the list of elements"

def test_writeData_FileNotFound(db_resource,db_client,table_mock,name_table,db_botoObj,tmpdir,tmp_folder_name):
	tmp_file_path = tmpdir.mkdir(tmp_folder_name)/"data.json"
	mess, code = db_botoObj.writeData(name_table,tmp_file_path,table_mock)
	assert code == 707
	assert mess == "File NotFound Error"

def test_writeData_ErrorDecodeJSON(db_resource,db_client,table_mock,name_table,db_botoObj,tmpdir,tmp_folder_name, data_json):
	text = ["hello"]
	tmp_file_path = tmpdir.mkdir(tmp_folder_name)/"data.json"
	tmp_file_path.write(text)
	mess, code = db_botoObj.writeData(name_table,tmp_file_path,table_mock)
	assert code == 808
	assert mess == f"Error Decode JSON file"

def test_writeData_NoTable(db_resource,db_client,table_mock,name_table,db_botoObj,tmpdir,tmp_folder_name,data_json):
	
	tmp_file_path = tmpdir.mkdir(tmp_folder_name)/"data.json"
	tmp_file_path.write(data_json)
	try:
		mess, code = db_botoObj.writeData(name_table,tmp_file_path)
	except SystemExit as error:
		code = error.args[1]
		mess = error.args[0]
	assert code == 606
	assert mess == f"Requested resource not found"
	 


def test_deleteTable(db_client, db_botoObj, table_mock, name_table):
		mess, code = db_botoObj.deleteTable(name_table,table_mock)
		assert mess == "Success Delete Table"
		assert code == 200
		res = db_client.list_tables()
		assert res['TableNames'] != [name_table]

def test_deleteTable_ResourceNotFound(db_client, db_botoObj, table_mock, name_table):
	try:
		mess, code = db_botoObj.deleteTable(name_table)
	except SystemExit as error:
		code = error.args[1]
		mess = error.args[0]
	
	assert code == 606
	assert mess == f"Resource [{name_table}] Not found"

def test_deleteTable_ParameterNoCorrect(db_client, db_botoObj):
	name = "Mo"
	mess, code = db_botoObj.deleteTable(name)

	assert code == 606
	assert mess == f"The parameters you provider are incorrect: Parameter validation failed:\n"+"Invalid length for parameter TableName, value: 2, valid min length: 3"

# test parametrici per query con una  Clausola 1 (solo con espressione relativa alla Key di Partizione)
clause_data_C1 = [
	(
	 [{"attribute":"id_device" , "op_comparison":"EQ" , "values":["D_HomeQ-01"] } ], # esiste l'elemento per key
	 [],[],[],"",10,200
	),
	(	 
	 [{"attribute":"id_device" , "op_comparison":"EQ" , "values":["D_HomeQ-02"] } ], # esiste
	 [],[],[],"",5,200
	),
	(	 
	 [{"attribute":"id_device" , "op_comparison":"EQ" , "values":["D_HomeQ-03"] } ],# NON ESISTono elementi per id_device
	 [],[],[],"",0,200
	)

]

@pytest.mark.parametrize("list_clause_key, list_op_coditional,list_clause_filter, list_op_coditional_filter,select, expected_num_row, expected_code", clause_data_C1)
def test_prepare_send_Query_Clause1(db_client, db_resource,db_botoObj,name_table, table_mock_with_data,  list_clause_key, list_op_coditional,list_clause_filter, list_op_coditional_filter,select, expected_num_row,expected_code):

		dataframe, code = db_botoObj.prepare_send_Query(name_table, list_clause_key, list_op_coditional,list_clause_filter, list_op_coditional_filter,select, table_mock_with_data)

		assert code == expected_code
		assert len(dataframe.axes[0]) == expected_num_row 

# test parametrici per query con una  Clausola 1 (solo con espressione relativa alla Key di Partizione)
clause_data_C1_Error = [
	(
	 [{"attribute":"attribute_not_present" , "op_comparison":"EQ" , "values":["D_HomeQ-01"] } ], # attributo non corrispone
	 [],[],[],"",
	 f"Error: in execution of query. Query condition missed key schema element: id_device",
	 606
	),
	(	 
	 [{"attribute":"id_device" , "op_comparison":"OP_NOT_VALID" , "values":["D_HomeQ-02"] } ], # errore operatore
	 [],[],[],"",
	 f"Error, no correct (op_comparison) for Key Partition",
	 606
	),
	(	 
	 [{"attribute":"id_device" , "op_comparison":"BETWEEN" , "values":["D_HomeQ-02"] } ], # errore operatore non supportato
	 [],[],[],"",
	 f"Error: in execution of query. Query key condition not supported",
	 606
	),


]
@pytest.mark.parametrize("list_clause_key, list_op_coditional,list_clause_filter, list_op_coditional_filter,select, expected_mess, expected_code", clause_data_C1_Error)
def test_prepare_send_Query_Clause1_Error(db_client, db_resource,db_botoObj,name_table, table_mock_with_data,  list_clause_key, list_op_coditional,list_clause_filter, list_op_coditional_filter,select, expected_mess, expected_code):

		# testare esecuzione query con 1 clousola
		try:
			mess, code = db_botoObj.prepare_send_Query(name_table, list_clause_key, list_op_coditional,list_clause_filter, list_op_coditional_filter,select, table_mock_with_data)
		except SystemExit as error:
			code = error.args[1]
			mess = error.args[0]

		assert code == expected_code
		assert mess == expected_mess


	 
clause_data_C1_C2 = [
	(
	 [{"attribute":"id_device" , "op_comparison":"EQ" , "values":["D_HomeQ-01"] },
	  {"attribute":"time_id" , "op_comparison":"EQ" , "values":["2021-09-30T21:16:02"] }
	 ], 
	 ["AND"],[],[],"",1,200 # key range con eq
	),
	(	 
	 [{"attribute":"id_device" , "op_comparison":"EQ" , "values":["D_HomeQ-02"] },
	  {"attribute":"time_id" , "op_comparison":"BETWEEN" , "values":["2021-09-30T21:10:01","2021-09-30T21:13:02"] }
	 ], 
	 [],[],[],"",3,200 # key range con between , anche in assenza di operatore conditionale il programma mette "and"
	),
	(	 
	 [{"attribute":"id_device" , "op_comparison":"EQ" , "values":["D_HomeQ-03"] },
	  {"attribute":"time_id" , "op_comparison":"EQ" , "values":["2021-09-30T21:10:01"] }
	 ],
	 ["AND"],[],[],"",0,200 # id_device non presente
	),
	(	 
	 [{"attribute":"id_device" , "op_comparison":"EQ" , "values":["D_HomeQ-03"] },
	  {"attribute":"time_id" , "op_comparison":"EQ" , "values":["2021-09"] }
	 ],
	 ["AND"],[],[],"",0,200 # 
	),
	(	 
	 [{"attribute":"id_device" , "op_comparison":"EQ" , "values":["D_HomeQ-02"] },
	  {"attribute":"time_id" , "op_comparison":"BEGINS_WITH" , "values":["2021-09-30T21:09"] }
	 ],
	 ["AND"],[],[],"",1,200 #  key range con begins_with
	),
	(	 
	 [{"attribute":"id_device" , "op_comparison":"EQ" , "values":["D_HomeQ-02"] },
	  {"attribute":"time_id" , "op_comparison":"IN" , "values":["2021-09-30T21:12:02,2021-09-30T21:13:02,value_other"] }
	 ],
	 ["AND"],[],[],"",2,200 #  key range con IN
	),
	(	 
	 [{"attribute":"id_device" , "op_comparison":"EQ" , "values":["D_HomeQ-01"] },
	  {"attribute":"time_id" , "op_comparison":"CONTAINS" , "values":["21:08"] }
	 ],
	 ["AND"],[],[],"",
	 f"Error: in execution of query. Invalid KeyConditionExpression: Invalid function name; function: contains", #  key range con CONTAINS",
	 606
	),  # key range con CONTAINS (non è utilizzabile nelle clausole relative a key)
	(	 
	 [{"attribute":"id_device" , "op_comparison":"EQ" , "values":["D_HomeQ-02"] },
	  {"attribute":"time_id" , "op_comparison":"BEGINS_WITH" , "values":["2021-09-30T21"] }
	 ],
	 ["AND"],[],[],"",
	 5,
	 200
	), 
	(	 
	 [{"attribute":"id_device" , "op_comparison":"EQ" , "values":["D_HomeQ-02"] },
	  {"attribute":"time_id" , "op_comparison":"IN" , "values":["2021,2022,2023"] }
	 ], 
	 ["AND"],[],[],"",0,200 # key range con begins_with , il valore è una stringa, si è dato  una lista  di  valore decimale, quindi non corrisponde nessun elemento 
	),

]
@pytest.mark.parametrize("list_clause_key, list_op_coditional,list_clause_filter, list_op_coditional_filter,select, expected_num_row, expected_code", clause_data_C1_C2)
def test_prepare_send_Query_Clause1_Clause2(db_client, db_resource,db_botoObj,name_table, table_mock_with_data,   list_clause_key, list_op_coditional,list_clause_filter, list_op_coditional_filter,select, expected_num_row,expected_code):
		#print( list_clause_key,file=sys.stderr)

		# testare esecuzione query con clousola C1 e C2
		try:
			dataframe, code = db_botoObj.prepare_send_Query(name_table, list_clause_key, list_op_coditional,list_clause_filter, list_op_coditional_filter,select, table_mock_with_data)
		except SystemExit as error:
			dataframe = error.args[0]
			code = error.args[1]
			assert code == expected_code
			assert dataframe == expected_num_row
		else: 
			assert code == expected_code
			assert len(dataframe.axes[0]) == expected_num_row 

clause_data_C1_C2_Error = [
	(
	 [{"attribute":"id_device" , "op_comparison":"EQ" , "values":["D_HomeQ-01"] },
	  {"attribute":"attr_other" , "op_comparison":"EQ" , "values":["2021-09-30T21:16:02"] }
	 ], 
	 ["AND"],[],[],"",
	 f"Error: in execution of query. Query condition missed key schema element: time_id",
	 606 # 
	),
	(
	 [{"attribute":"id_device" , "op_comparison":"IN" , "values":["D_HomeQ-01, D_HomeQ-02"] },
	  {"attribute":"time_id" , "op_comparison":"EQ" , "values":["2021-09-30T21:16:02"] }
	 ], 
	 ["AND"],[],[],"",
	 f"Error formulate Query in clause key Partition",
	 606 # 
	),
	(
	 [{"attribute":"id_device" , "op_comparison":"BETWEEN" , "values":["D_HomeQ-01, D_HomeQ-02"] },
	  {"attribute":"time_id" , "op_comparison":"EQ" , "values":["2021-09-30T21:16:02"] }
	 ], 
	 ["AND"],[],[],"",
	 f"Error formulate Query in clause key Partition",
	 606 # 
	),
	(	 
	 [{"attribute":"id_device" , "op_comparison":"EQ" , "values":["D_HomeQ-02"] },
	  {"attribute":"time_id" , "op_comparison":"BETWEEN" , "values":["2021","2022"] }
	 ], 
	 [],[],[],"","TypeError in value of attribute",606 # key range con between , il valore è una stringa, si è dato un valore decimale, quindi errore 
	),
	(	 
	 [{"attribute":"id_device" , "op_comparison":"EQ" , "values":["D_HomeQ-02"] },
	  {"attribute":"time_id" , "op_comparison":"BEGINS_WITH" , "values":["2021"] }
	 ], 
	 [],[],[],"","TypeError in value of attribute",606 # key range con begins_with , il valore è una stringa, si è dato un valore decimale, quindi errore 
	),
	(
	 [{"attribute":"id_device" , "op_comparison":"EQ" , "values":["D_HomeQ-01"] },
	  {"attribute":"time_id" , "op_comparison":"LT" , "values":["2021"] }
	 ], 
	 ["AND"],
	 [],
	 [],"",
	 "TypeError in value of attribute",
	 606 # 
	)
]
@pytest.mark.parametrize("list_clause_key, list_op_coditional,list_clause_filter, list_op_coditional_filter,select, expected, expected_code", clause_data_C1_C2_Error)
def test_prepare_send_Query_Clause1_Clause2_Error(db_client, db_resource,db_botoObj,name_table, table_mock_with_data, data_device_two,  list_clause_key, list_op_coditional,list_clause_filter, list_op_coditional_filter,select, expected,expected_code):

	try:
		mess, code = db_botoObj.prepare_send_Query(name_table, list_clause_key, list_op_coditional,list_clause_filter, list_op_coditional_filter,select, table_mock_with_data)
	except SystemExit as error:
		mess = error.args[0]
		code = error.args[1]
		assert code == expected_code
		assert mess == expected
	else:
		assert code == expected_code
		assert mess == expected


clause_data_C1_C2_F1_F2 = [
	(
	 [{"attribute":"id_device" , "op_comparison":"EQ" , "values":["D_HomeQ-01"] },
	  {"attribute":"time_id" , "op_comparison":"EQ" , "values":["2021-09-30T21:08:02"] }
	 ], 
	 ["AND"],
	 [{"attribute":"ping_ms" , "op_comparison":"EQ" , "values":["17.28"]}],
	 [],"",
	 1,
	 200 # 
	),
	(
	 [{"attribute":"id_device" , "op_comparison":"EQ" , "values":["D_HomeQ-01"] },
	  {"attribute":"time_id" , "op_comparison":"BETWEEN" , "values":["2021-09-30T21:09:02", "2021-09-30T21:13:02"] }
	 ], 
	 ["AND"],
	 [{"attribute":"humidity_p" , "op_comparison":"GT" , "values":["41.0"]}],
	 [],"",
	 2,
	 200 # 
	),
	(
	 [{"attribute":"id_device" , "op_comparison":"EQ" , "values":["D_HomeQ-02"] },
	  {"attribute":"time_id" , "op_comparison":"BETWEEN" , "values":["2021-09-30T21:08:02", "2021-09-30T21:18:02"] }
	 ], 
	 ["AND"],
	 [{"attribute":"time_id" , "op_comparison":"CONTAINS" , "values":["21:0"]}],
	 [],"",
	 2,
	 200 # 
	),
	(
	 [{"attribute":"id_device" , "op_comparison":"EQ" , "values":["D_HomeQ-02"] },
	  {"attribute":"time_id" , "op_comparison":"BETWEEN" , "values":["2021-09-30T21:08:02", "2021-09-30T21:18:02"] }
	 ], 
	 ["AND"],
	 [{"attribute":"time_id" , "op_comparison":"BEGINS_WITH" , "values":["2021-09-30T21:1"]}],
	 [],"",
	 3,
	 200 # 
	),
	(
	 [{"attribute":"id_device" , "op_comparison":"EQ" , "values":["D_HomeQ-01"] },
	  {"attribute":"time_id" , "op_comparison":"BETWEEN" , "values":["2021-09-30T21:09:02", "2021-09-30T21:13:02"] }
	 ], 
	 ["AND"],
	 [{"attribute":"humidity_p" , "op_comparison":"GT" , "values":["41.0"]},
	  {"attribute":"temperature_c" , "op_comparison":"LT" , "values":["26.0"]}
	 ],
	 [],"",
	 1,
	 200 # 
	),
	(
	 [{"attribute":"id_device" , "op_comparison":"EQ" , "values":["D_HomeQ-01"] },
	  {"attribute":"time_id" , "op_comparison":"BETWEEN" , "values":["2021-09-30T21:09:02", "2021-09-30T21:13:02"] }
	 ], 
	 ["AND"],
	 [{"attribute":"humidity_p" , "op_comparison":"IN" , "values":["41.0,42.0"]},
	  {"attribute":"temperature_c" , "op_comparison":"LT" , "values":["26.0"]}
	 ],
	 [],"",
	 2,
	 200 # 
	),
	(
	 [{"attribute":"id_device" , "op_comparison":"EQ" , "values":["D_HomeQ-02"] },
	  {"attribute":"time_id" , "op_comparison":"BETWEEN" , "values":["2021-09-30T21:09:02", "2021-09-30T21:13:02"] }
	 ], 
	 ["AND"],
	 [
	  {"attribute":"humidity_p" , "op_comparison":"LE" , "values":["26.0"]},
	  {"attribute":"temperature_c" , "op_comparison":"IN" , "values":["41.0,42.0,43.0,45.0"]},
	 ],
	 [],"",
	 4,
	 200 # 
	),
	(
	 [{"attribute":"id_device" , "op_comparison":"EQ" , "values":["D_HomeQ-02"] },
	  {"attribute":"time_id" , "op_comparison":"BETWEEN" , "values":["2021-09-30T21:09:02", "2021-09-30T21:13:02"] }
	 ], 
	 ["AND"],
	 [
	  {"attribute":"humidity_p" , "op_comparison":"LE" , "values":["26.0"]},
	  {"attribute":"temperature_c" , "op_comparison":"BETWEEN" , "values":["41.0","45.0"]},
	 ],
	 [],"",
	 4,
	 200
	)
]

@pytest.mark.parametrize("list_clause_key, list_op_coditional,list_clause_filter, list_op_coditional_filter,select, expected, expected_code", clause_data_C1_C2_F1_F2)
def test_prepare_send_Query_Clause1_Clause2_Filter1_Filter2(db_client, db_resource,db_botoObj,name_table, table_mock_with_data,   list_clause_key, list_op_coditional,list_clause_filter, list_op_coditional_filter,select, expected,expected_code):
	mess, code = db_botoObj.prepare_send_Query(name_table, list_clause_key, list_op_coditional,list_clause_filter, list_op_coditional_filter,select, table_mock_with_data)
	assert code == expected_code
	assert len(mess.axes[0]) == expected


clause_data_C1_C2_F1_F2_NoItem = [
	(
	 [{"attribute":"id_device" , "op_comparison":"EQ" , "values":["D_HomeQ-01"] },
	  {"attribute":"time_id" , "op_comparison":"EQ" , "values":["2021-09-30T21:08:02"] }
	 ], 
	 ["AND"],
	 [{"attribute":"ping_ms" , "op_comparison":"EQ" , "values":["17.28"]},
	  {"attribute":"attr_other" , "op_comparison":"LT" , "values":["26.0"]} # non è resente il campo
	 ],
	 [],"",
	 0,
	 200 # 
	),
	(
	 [{"attribute":"id_device" , "op_comparison":"EQ" , "values":["D_HomeQ-03"] }, #non è presente il device con quel id
	  {"attribute":"time_id" , "op_comparison":"EQ" , "values":["2021-09-30T21:08:02"] }
	 ], 
	 ["AND"],
	 [{"attribute":"ping_ms" , "op_comparison":"EQ" , "values":["17.28"]},
	  {"attribute":"temperature_c" , "op_comparison":"LT" , "values":["26.0"]} #
	 ],
	 ["and"],"",
	 0,
	 200 
	),
	(
	 [{"attribute":"id_device" , "op_comparison":"EQ" , "values":["D_HomeQ-01"] },
	  {"attribute":"time_id" , "op_comparison":"BETWEEN" , "values":["2021-09-30T21:08:02", "2021-09-30T21:28:02"] }
	 ], 
	 ["AND"],
	 [{"attribute":"ping_ms" , "op_comparison":"LT" , "values":["20.00"]},
	  {"attribute":"humidity_p" , "op_comparison":"BEGINS_WITH" , "values":["3"]} 
	 ],
	 [],"",
	 0,
	 200 # 
	)
]
@pytest.mark.parametrize("list_clause_key, list_op_coditional,list_clause_filter, list_op_coditional_filter,select, expected, expected_code", clause_data_C1_C2_F1_F2_NoItem)
def test_prepare_send_Query_Clause1_Clause2_Filter1_Filter2_NoItem(db_client, db_resource,db_botoObj,name_table, table_mock_with_data,  list_clause_key, list_op_coditional,list_clause_filter, list_op_coditional_filter,select, expected,expected_code):
		
		
		mess, code = db_botoObj.prepare_send_Query(name_table, list_clause_key, list_op_coditional,list_clause_filter, list_op_coditional_filter,select, table_mock_with_data)

		assert code == expected_code
		assert len(mess.axes[0]) == expected


clause_data_C1_C2_F1_F2_NoTable= [
	(
	 [{"attribute":"id_device" , "op_comparison":"EQ" , "values":["D_HomeQ-01"] },
	  {"attribute":"time_id" , "op_comparison":"EQ" , "values":["2021-09-30T21:08:02"] }
	 ], 
	 ["AND"],
	 [{"attribute":"ping_ms" , "op_comparison":"EQ" , "values":["17.28"]},
	  {"attribute":"attr_other" , "op_comparison":"LT" , "values":["26.0"]} # non è resente il campo
	 ],
	 [],"",
	 "Error: in execution of query. Requested resource not found",
	 606 
	)]
@pytest.mark.parametrize("list_clause_key, list_op_coditional,list_clause_filter, list_op_coditional_filter,select, expected, expected_code", clause_data_C1_C2_F1_F2_NoTable )
def test_prepare_send_Query_Clause1_Clause2_Filter1_Filter2_NoTable(db_client, db_resource,db_botoObj,name_table, table_mock_with_data,  list_clause_key, list_op_coditional,list_clause_filter, list_op_coditional_filter,select, expected,expected_code):
	try:
		mess, code = db_botoObj.prepare_send_Query(name_table, list_clause_key, list_op_coditional,list_clause_filter, list_op_coditional_filter,select)
	except SystemExit as error:
		mess = error.args[0]
		code = error.args[1]

		assert code == expected_code
		assert mess == expected



clause_data_C2_F1_F2_Scan= [
	(
	 [{"attribute":"time_id" , "op_comparison":"BETWEEN" , "values":["2021-09-30T21:09:02","2021-09-30T21:15:01"] }
	 ], 
	 [],
	 "",
	 10,
	 200  
	),
	(
	 [{"attribute":"time_id" , "op_comparison":"CONTAINS" , "values":["08:02"] }
	 ], 
	 [],
	 "",
	 2,
	 200 
	),
	(
	 [{"attribute":"time_id" , "op_comparison":"CONTAINS" , "values":["08:02"] },
	  {"attribute":"humidity_p" , "op_comparison":"LT" , "values":["30.0"] }
	 ], 
	 [],
	 "",
	 1,
	 200 
	),
	(
	 [{"attribute":"time_id" , "op_comparison":"BETWEEN" , "values":["2021-09-30T21:12:02","2021-09-30T21:18:02"] },
	  {"attribute":"humidity_p" , "op_comparison":"LT" , "values":["43.0"] },
	  {"attribute":"ping_ms" , "op_comparison":"IN" , "values":["21.35,18.59"] },
	 ], 
	 [],
	 "",
	 1,
	 200 # 
	),
	(
	 [{"attribute":"time_id" , "op_comparison":"BETWEEN" , "values":["2021-09-30T21:12:02","2021-09-30T21:18:02"] },
	  {"attribute":"humidity_p" , "op_comparison":"BEGINS_WITH" , "values":["4"] },
	  {"attribute":"ping_ms" , "op_comparison":"IN" , "values":["21.35,18.59"] },
	 ], 
	 [],
	 "",
	 0,
	 200 # 
	),
	(
	 [{"attribute":"time_id" , "op_comparison":"BETWEEN" , "values":["2021-09-30T21:12:02","2021-09-30T21:18:02"] },
	  {"attribute":"humidity_p" , "op_comparison":"LT" , "values":["43.0"] },
	  {"attribute":"ping_ms" , "op_comparison":"BETWEEN" , "values":["20.35","22.0"] },
	 ], 
	 ["and"],
	 "",
	 1,
	 200 # 
	),

]
@pytest.mark.parametrize("list_clause_filter, list_op_coditional_filter,select, expected, expected_code", clause_data_C2_F1_F2_Scan)
def test_prepare_send_Scan(db_client, db_resource,db_botoObj,name_table, table_mock_with_data,  list_clause_filter, list_op_coditional_filter,select, expected,expected_code):
	mess, code = db_botoObj.prepare_send_Scan(name_table,list_clause_filter, list_op_coditional_filter,select, table_mock_with_data)
	assert code == expected_code
	assert len(mess.axes[0]) == expected

clause_data_C1_C2_Select =[(	 
	 [{"attribute":"id_device" , "op_comparison":"EQ" , "values":["D_HomeQ-02"] },
	  {"attribute":"time_id" , "op_comparison":"BETWEEN" , "values":["2021-09-30T21:10:01", "2021-09-30T21:19:01"] }
	 ],
	 ["AND"],[],[],
	 "temperature_c,humidity_p",
	 3,
	 2,
	 200
	), 
	(	 
	 [{"attribute":"id_device" , "op_comparison":"EQ" , "values":["D_HomeQ-02"] },
	  {"attribute":"time_id" , "op_comparison":"BETWEEN" , "values":["2021-09-30T21:10:01", "2021-09-30T21:19:01"] }
	 ],
	 ["AND"],[],[],
	 "temperature_c,humidity_p,no_attribute",
	 3,
	 3,
	 200
	), 
]
@pytest.mark.parametrize("list_clause_key, list_op_coditional,list_clause_filter, list_op_coditional_filter,select, expected_num_row, expected_col,expected_code", clause_data_C1_C2_Select)
def test_prepare_send_Query_Select(db_client, db_resource,db_botoObj,name_table, table_mock_with_data, list_clause_key, list_op_coditional,list_clause_filter, list_op_coditional_filter,select, expected_num_row,expected_col, expected_code):
	mess, code = db_botoObj.prepare_send_Query(name_table, list_clause_key, list_op_coditional,list_clause_filter, list_op_coditional_filter,select, table_mock_with_data)
	assert code == expected_code
	assert len(mess.axes[0]) == expected_num_row
	assert len(mess.axes[1]) == expected_col

clause_data_C2_F1_F2_Select= [
	([{"attribute":"time_id" , "op_comparison":"BETWEEN" , "values":["2021-09-30T21:12:02","2021-09-30T21:18:02"] },
	  {"attribute":"humidity_p" , "op_comparison":"LT" , "values":["43.0"] },
	  {"attribute":"ping_ms" , "op_comparison":"IN" , "values":["21.35,18.59"] },
	 ], 
	 [],
	 "ping_ms,humidity_p",
	 1,
	 2,
	 200 # 
	),
]
@pytest.mark.parametrize("list_clause_filter, list_op_coditional_filter,select, expected_row, expected_col, expected_code", clause_data_C2_F1_F2_Select)
def test_prepare_send_Scan_Select(db_client, db_resource,db_botoObj,name_table, table_mock_with_data,  list_clause_filter, list_op_coditional_filter,select, expected_row,expected_col, expected_code):
	mess, code = db_botoObj.prepare_send_Scan(name_table,list_clause_filter, list_op_coditional_filter,select, table_mock_with_data)
	assert code == expected_code
	assert len(mess.axes[0]) == expected_row
	assert len(mess.axes[1]) == expected_col


clause_data_C2_F1_F2_Noattribute = [
	([{"attribute":"time_id" , "op_comparison":"BETWEEN" , "values":["2021-09-30T21:12:02","2021-09-30T21:18:02"] },
	  {"attribute":"humidity_p" , "op_comparison":"LT" , "values":["43.0"] },
	  {"attribute":"attr_other" , "op_comparison":"IN" , "values":["21.35,18.59"] },
	 ], 
	 [],
	 "ping_ms,humidity_p",
	 0,
	 2,
	 200 # 
	),
]
@pytest.mark.parametrize("list_clause_filter, list_op_coditional_filter,select, expected_row, expected_col, expected_code", clause_data_C2_F1_F2_Noattribute)
def test_prepare_send_Scan_Noattribute(db_client, db_resource,db_botoObj, name_table, table_mock_with_data, list_clause_filter, list_op_coditional_filter,select, expected_row,expected_col, expected_code):
	mess, code = db_botoObj.prepare_send_Scan(name_table,list_clause_filter, list_op_coditional_filter,select, table_mock_with_data)
	assert code == expected_code
	assert len(mess.axes[0]) == expected_row
	assert len(mess.axes[1]) == expected_col


parameter = [
	("Model_Device_1", True, f"Correct compile template", 200),
	("Model_Not_Match", False, f"The value in the field [sensorDeviceModel] does not match with [Model_Not_Match]",808)
]
@pytest.mark.parametrize("sensorDevice_model,expected_bool_resp,expected_mess, expected_code", parameter)
def test_parseJsonTemplate(db_botoObj,tmpdir,json_file_template,sensorDevice_model,expected_bool_resp,expected_mess, expected_code):
	
	bool_resp, mess, code = db_botoObj.parseJsonTemplate("file_template.json",sensorDevice_model,json_file_template)
	assert bool_resp == expected_bool_resp
	assert mess == expected_mess
	assert code == expected_code



def test_parseJsonTemplate_Error(db_botoObj,tmpdir,json_file_template_2, name_table):
	sensorDevice_model = name_table

	bool_resp, mess, code = db_botoObj.parseJsonTemplate("file_template.json",sensorDevice_model,json_file_template_2)
	assert bool_resp == False
	assert mess == f"Error, the items in the fields ('keys') and ('keys_measures') does not match"
	assert code == 808

def test_parseJsonTemplate_Error2(db_botoObj,tmpdir,json_file_template_3, name_table):
	sensorDevice_model = name_table

	bool_resp, mess, code = db_botoObj.parseJsonTemplate("file_template.json",sensorDevice_model,json_file_template_3)
	assert bool_resp == False
	assert mess == f"Error in key type definition, choose ( partition || sort )"
	assert code == 808

def test_parseJsonTemplate_Error3(db_botoObj,tmpdir,json_file_template_4, name_table):
	sensorDevice_model = name_table

	bool_resp, mess, code = db_botoObj.parseJsonTemplate("file_template.json",sensorDevice_model,json_file_template_4)
	assert bool_resp == False
	assert mess == f"The field [sensorDeviceModel] not is present"
	assert code == 808

def test_parseJsonTemplate_Error4(db_botoObj,tmpdir,json_file_template_5, name_table):
	sensorDevice_model = name_table

	bool_resp, mess, code = db_botoObj.parseJsonTemplate("file_template.json",sensorDevice_model,json_file_template_5)
	assert bool_resp == False
	assert mess == f"Error, the number of items in the fields ('keys') and ('keys_measures') does not match "
	assert code == 808

def test_parseJsonTemplate_Error5(db_botoObj,tmpdir,json_file_template_6, name_table):
	sensorDevice_model = name_table

	bool_resp, mess, code = db_botoObj.parseJsonTemplate("file_template.json",sensorDevice_model,json_file_template_6)
	assert bool_resp == False
	assert mess == f"Uncorrect compile template, the required fields are not present, insert (sensorDevice_model, keys, keys_measures)"
	assert code == 808

def test_parseJsonTemplate_ErrorParseJSON(db_botoObj,name_table,tmpdir,tmp_folder_name):
	text_1 = ["hello"]
	tmp_file_path = tmpdir.mkdir(tmp_folder_name)/"data.json"
	tmp_file_path.write(text_1)
	bool_resp, mess, code = db_botoObj.parseJsonTemplate("data.json",name_table,tmp_file_path)
	assert bool_resp == False
	assert mess == "Error convert to dictionary for Json Object"
	assert code == 808