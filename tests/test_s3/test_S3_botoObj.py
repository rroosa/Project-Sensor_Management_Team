import pytest
from tempfile import NamedTemporaryFile
import os
import sys
from botocore.exceptions import ClientError
sys.path.append("../../")
from ServiceS3.S3_botoObj import S3BotoObj

file_folder = [
	("fileHello", None, "fileHello"),
	("fileHello", "hello","hello/fileHello" ),

]

@pytest.mark.parametrize("object_name, folder, key", file_folder)
def test_upload_file_in_S3(s3_botoObj,s3_client, tmpdir,tmp_folder_name,bucket_mock,bucket_name,object_name,folder,key):
	# creazione della cartella temporanea e file
	#file_tmp = tmpdir.mkdir(tmp_folder_name).join("hello.txt")
	#file_tmp.write("content")
	tmp_file_path = tmpdir.mkdir(tmp_folder_name)/"hello.txt"
	tmp_file_path.write("content")
	"""	
	with NamedTemporaryFile(delete=True, suffix=".txt") as tmp:
		with open(tmp.name, "w", encoding="UTF-8") as f:
			f.write("content")
			f.save(tmp_file_path)"""

	message, code = s3_botoObj.upload_file_in_S3(bucket_name,tmp_file_path,object_name,folder)

	assert code == 200

	resp = s3_client.get_object(Bucket=bucket_name, Key=key)
	
	assert resp["ContentLength"] == 7
	assert resp["Body"].read() == b"content"

	objects = s3_client.list_objects_v2(Bucket=bucket_name).get("Contents")
	assert len(objects) == 1

def test_upload_file_in_S3_BucketNotExist(s3_botoObj,s3_client, tmpdir,tmp_folder_name):
	
	tmp_file_path = tmpdir.mkdir(tmp_folder_name)/"hello.txt"
	tmp_file_path.write("content")
	bucket_name = "bucket_notExists"
	message, code = s3_botoObj.upload_file_in_S3(bucket_name,tmp_file_path,"fileHello",None)
	
	assert code == 400
	assert message == "Bucket Not Found"
	with pytest.raises(ClientError) as er:
		objects = s3_client.list_objects_v2(Bucket=bucket_name).get("Contents")
	assert er.value.response["Error"]["Code"] == "NoSuchBucket"
	assert er.value.response["Error"]["Message"] == (
			"The specified bucket does not exist"
		)

def test_upload_file_in_S3_ErrorName(s3_botoObj,s3_client, tmpdir,tmp_folder_name):
	
	tmp_file_path = tmpdir.mkdir(tmp_folder_name)/"hello.txt"
	tmp_file_path.write("content")
	bucket_name = "bu"
	message, code = s3_botoObj.upload_file_in_S3(bucket_name,tmp_file_path,"fileHello",None)
	
	assert code == 400
	assert message == "Bucket Not Found"



def test_upload_file_in_S3_FileNotPresent(s3_botoObj,s3_client, tmpdir,tmp_folder_name,bucket_mock,bucket_name):
	
	tmp_file_path = tmpdir.mkdir(tmp_folder_name)/"hello.txt"
	
	 
	message, code = s3_botoObj.upload_file_in_S3(bucket_name,tmp_file_path,"fileHello",None)
	
	assert code == 606
	assert message == "file is not present in local"

	objects = s3_client.list_objects_v2(Bucket=bucket_name).get("Contents")
	assert objects is None


def test_download_file_from_S3(s3_botoObj, s3_client, bucket_mock, bucket_name, tmpdir,tmp_folder_name):
	
	# creazione di un file temporanea da essere inserito nel bucket_mock
    file_text = "text of file1"
    with NamedTemporaryFile(delete=True, suffix=".txt") as tmp:
        with open(tmp.name, "w", encoding="UTF-8") as f:
            f.write(file_text)

        s3_client.upload_file(tmp.name, bucket_name, "file1")

    # testare la funzione di download file contenuto direttamente nel bucket 
    tmp_file_path = tmpdir.mkdir(tmp_folder_name) # cartella temporale per salvare il file una volta scaricato
    bool_resp, message, code = s3_botoObj.download_file_from_S3(bucket_name, None,"file1")

    assert bool_resp == True
    assert code == 200
    assert message == "Success"

def test_download_file_from_S3_BucketNotExists(s3_botoObj, s3_client, tmpdir,tmp_folder_name):
	bucket_name = "bucket_notExists"
	# creazione della cartella temporanea dove caricare l'eventuale file scaricato
	tmp_file_path = tmpdir.mkdir(tmp_folder_name)
	bool_resp, message, code = s3_botoObj.download_file_from_S3(bucket_name, None,"file1")

	assert bool_resp == False
	assert message == "The specified bucket does not exist"
	assert code == "NoSuchBucket"

	with pytest.raises(ClientError) as er:
		objects = s3_client.list_objects_v2(Bucket=bucket_name).get("Contents")
	assert er.value.response["Error"]["Code"] == "NoSuchBucket"
	assert er.value.response["Error"]["Message"] == (
			"The specified bucket does not exist"
		)

def test_download_file_from_S3_FileNotExists(s3_botoObj, s3_client,bucket_mock, bucket_name,tmpdir,tmp_folder_name):
	
	key_name = "file_notExists"
	# creazione della cartella temporanea dove caricare l'eventuale file scaricato
	tmp_file_path = tmpdir.mkdir(tmp_folder_name)
	bool_resp, message, code = s3_botoObj.download_file_from_S3(bucket_name, None,key_name)

	assert bool_resp == False
	assert message == "Not Found"
	assert code == '404'


def test_download_file_from_S3_with_prefix(s3_botoObj, s3_client, bucket_mock, bucket_name, tmpdir,tmp_folder_name):

	# creazione di un file temporanea da essere inserito nel bucket_mock
    file_text = "text of file1"
    folder = "folder"
    file_name = "file1"

    key = f"{folder}/{file_name}"

    with NamedTemporaryFile(delete=True, suffix=".txt") as tmp:
        with open(tmp.name, "w", encoding="UTF-8") as f:
            f.write(file_text)

        s3_client.upload_file(tmp.name, bucket_name, key)

    # testare la funzione di download file contenuto direttamente nel bucket 
    tmp_file_path = tmpdir.mkdir(tmp_folder_name) # cartella temporale per salvare il file una volta scaricato
    bool_resp, message, code = s3_botoObj.download_file_from_S3(bucket_name, folder,file_name)

    assert bool_resp == True
    assert code == 200
    assert message == "Success"

    objects = s3_client.list_objects_v2( Bucket=bucket_name, Delimiter = '/', Prefix = f"{folder}/")
    print(objects)
    elements = objects.get("Contents")
    assert len(elements) == 1


def test_delete_file_from_s3(s3_botoObj, s3_client, bucket_mock,bucket_name):
	# inserire un file nel bucket
	file_text = "text of file1"
	object_name = "file1"
	with NamedTemporaryFile(delete=True, suffix=".txt") as tmp:
		with open(tmp.name, "w", encoding="UTF-8") as f:
			f.write(file_text)

		s3_client.upload_file(tmp.name, bucket_name, object_name)

	# ottieni file prima della eliminazione
	resp = s3_client.get_object(Bucket = bucket_name, Key = object_name)
	assert resp["Body"].read() == b"text of file1"


	# testare eliminazione
	message, code = s3_botoObj.delete_file_from_s3(bucket_name, object_name)
	assert message == f"Success Delete Object [{object_name}]"
	assert code == 200

	# ottieni file dopo eliminazione
	with pytest.raises(ClientError) as er:
		resp = s3_client.get_object(Bucket = bucket_name, Key = object_name)
	assert er.value.response["Error"]["Code"] == "NoSuchKey"
	assert er.value.response["Error"]["Message"] == (
			"The specified key does not exist."
		)

def test_delete_file_from_s3_BucketNotExists(s3_botoObj, s3_client):
	bucket_name = "bucket_notExists"
	object_name = "file1"
	message, code = s3_botoObj.delete_file_from_s3(bucket_name, object_name)
	assert message == "The specified bucket does not exist"
	assert code == 'NoSuchBucket'

def test_delete_file_from_s3_FileNotExists(s3_botoObj, s3_client,bucket_mock,bucket_name):
	object_name = "file_notExists"
	resp_bool, lista_mess = s3_botoObj.getObjectIntoCategory(bucket_name, None)
	assert resp_bool == False
	assert isinstance(lista_mess, str) 

	message, code = s3_botoObj.delete_file_from_s3(bucket_name, object_name)
	assert message == f"Success Delete Object [{object_name}]"
	assert code == 200

# test list only folders
def test_getListCategories(s3_botoObj, s3_client,bucket_mock,bucket_name):
	# creare piu file 
	file_text = "test"
	key_1 = f"folder1/file1-1"
	key_2 = f"folder2/file2-1"
	key_3 = f"folder1/file1-2"
	key_4 = f"folder2/file2-2"
	with NamedTemporaryFile(delete=True, suffix=".txt") as tmp:
		with open(tmp.name, "w", encoding="UTF-8") as f:
			f.write(file_text)

		s3_client.upload_file(tmp.name, bucket_name, key_1)
		s3_client.upload_file(tmp.name, bucket_name, key_2)
		s3_client.upload_file(tmp.name, bucket_name, key_3)
		s3_client.upload_file(tmp.name, bucket_name, key_4)
		result = s3_client.list_objects_v2( Bucket=bucket_name, Delimiter = '/')
		assert len(result.get('CommonPrefixes')) == 2
		resp_bool, content = s3_botoObj.getListCategories(bucket_name)
		assert resp_bool == True
		assert isinstance(content,list)
		assert len(content) == 2
		assert content[0] == 'folder1/'
		assert content[1] == 'folder2/'

# test list only folders, but there are not folders in bucket
def test_getListCategories_FolderNotPresent(s3_botoObj, s3_client,bucket_mock,bucket_name):
	# creare piu file 
	file_text = "test"
	key_1 = "file1"
	with NamedTemporaryFile(delete=True, suffix=".txt") as tmp:
		with open(tmp.name, "w", encoding="UTF-8") as f:
			f.write(file_text)

		s3_client.upload_file(tmp.name, bucket_name, key_1)
		result = s3_client.list_objects_v2( Bucket=bucket_name, Delimiter = '/')
		assert result.get('CommonPrefixes') is None 

		resp_bool, content = s3_botoObj.getListCategories(bucket_name)
		assert resp_bool == False
		assert content == "-"

# test list only folders, but bucket not exists
def test_getListCategories_BucketNotExist(s3_botoObj, s3_client):

	bucket_name = "bucket_notExists"
	resp_bool, content = s3_botoObj.getListCategories(bucket_name)
	assert resp_bool == False
	assert content == "The specified bucket does not exist"

 

def test_getObjectIntoCategory(s3_botoObj, s3_client,bucket_mock,bucket_name):
	file_text = "test"
	category = "folder1"
	key_1 = f"folder1/file1-1"
	key_2 = f"folder1/file1-2"
	key_3 = f"folder1/file1-3"
	key_4 = f"folder1/file1-4"
	with NamedTemporaryFile(delete=True, suffix=".txt") as tmp:
		with open(tmp.name, "w", encoding="UTF-8") as f:
			f.write(file_text)

		s3_client.upload_file(tmp.name, bucket_name, key_1)
		s3_client.upload_file(tmp.name, bucket_name, key_2)
		s3_client.upload_file(tmp.name, bucket_name, key_3)
		s3_client.upload_file(tmp.name, bucket_name, key_4)
		result = s3_client.list_objects_v2( Bucket=bucket_name, Delimiter = '/', Prefix= 'folder1/')
		elements = result.get('Contents')
		assert len(elements) == 4
		assert elements[0]['Key'] == "folder1/file1-1"


		resp_bool, content = s3_botoObj.getObjectIntoCategory(bucket_name,category)
		assert resp_bool == True
		assert isinstance(content,list)
		assert len(content) == 4
		assert content[0] == "file1-1"

def test_getObjectIntoCategory_CategoryNotPresent(s3_botoObj, s3_client,bucket_mock,bucket_name):
	category = "folder_notPresent"
	result = s3_client.list_objects_v2( Bucket=bucket_name, Delimiter = '/', Prefix= 'folder1/')
	elements = result.get('Contents')
	assert elements is None

	resp_bool, content = s3_botoObj.getObjectIntoCategory(bucket_name,category)
	assert resp_bool == False
	assert content == f"Object does not exist"

def test_getObjectIntoCategory_BucketNotExists(s3_botoObj, s3_client):
	category = "folder_notPresent"
	bucket_name = "bucket_notExists"
	resp_bool, content = s3_botoObj.getObjectIntoCategory(bucket_name,category)
	assert resp_bool == "NoSuchBucket"
	assert content == f"The specified bucket does not exist"

def test_exampleListObject_in_Bucket_with_Session(s3_botoObj, s3_client,bucket_mock,bucket_name):
	file_text = "test"
	category = "folder1"
	key_1 = f"folder1/file1-1"
	key_2 = f"folder1/file1-2"
	key_3 = f"folder1/file1-3"
	key_4 = f"folder1/file1-4"
	key_5 = f"folder2/file2-1"
	key_6 = f"folder2/file2-2"
	key_7 = f"folder2/file2-3"
	key_8 = f"folder2/file2-4"
	key_9 = f"file9"

	with NamedTemporaryFile(delete=True, suffix=".txt") as tmp:
		with open(tmp.name, "w", encoding="UTF-8") as f:
			f.write(file_text)

		s3_client.upload_file(tmp.name, bucket_name, key_1)
		s3_client.upload_file(tmp.name, bucket_name, key_2)
		s3_client.upload_file(tmp.name, bucket_name, key_3)
		s3_client.upload_file(tmp.name, bucket_name, key_4)
		s3_client.upload_file(tmp.name, bucket_name, key_5)
		s3_client.upload_file(tmp.name, bucket_name, key_6)
		s3_client.upload_file(tmp.name, bucket_name, key_7)
		s3_client.upload_file(tmp.name, bucket_name, key_8)
		s3_client.upload_file(tmp.name, bucket_name, key_9)


		result = s3_client.list_objects_v2( Bucket=bucket_name)
		elements = result.get('Contents')
		assert len(elements) == 9
		assert elements[0]['Key'] == "file9"
		assert elements[8]['Key'] == f"folder2/file2-4"

		resp = s3_botoObj.exampleListObject_in_Bucket_with_Session(bucket_name)
		assert resp == "Success"

def test_exampleListObject_in_Bucket_with_Session_Error(s3_botoObj, s3_client,bucket_mock):
	name = "bu"
	result = s3_botoObj.exampleListObject_in_Bucket_with_Session(name)
	assert result == f"The specified bucket does not exist"




test_data = [
		( "file.txt","text", "file.txt"),
		( "file.docx","word", "file.docx"),
		( "file","word", "file.docx"),
		( "file.pdf","pdf", "file.pdf"),
		( "file","pdf", "file.pdf"),
		( "file.png", "png",False),
		( "file.png", "text", None),
		( "file.txt", "word", None ),
		( "file.txt", "execl", False),
		( "file.txt", "word",None),
		( "file", "text","file.txt")

]

@pytest.mark.parametrize("object_name,type_file,expected", test_data)
def test_allowed_obj_type( object_name, type_file, expected, s3_botoObj):

	result = s3_botoObj.allowed_obj_type(object_name, type_file)
	assert result == expected

obj = [
	("file.txt", True),
	("file",False)
]
@pytest.mark.parametrize("object_name, expected",obj)
def test_allowed_obj(object_name, expected, s3_botoObj):
	result = s3_botoObj.allowed_obj(object_name)
	assert result == expected



test_data = [
		( "file.txt", ".txt", True),
		( "file.json", None,False),
		( "file.docx", ".docx",True),
		( "file.pdf", ".pdf", True),
		( "file.png",None,  False),
		( "file", None, False)
		
]

@pytest.mark.parametrize("file_name,getExt,expected", test_data)
def test_allowed_file_type_check_file(file_name, getExt,expected, s3_botoObj):

	result_bool = s3_botoObj.allowed_file_type(file_name,"check_file")
	assert result_bool == expected
	assert s3_botoObj.getExt() == getExt
	s3_botoObj.setExt(None)



test_data_2 = [
		( "file.txt", ".txt", "file.txt"),
		( "file.txt.pdf", ".txt", False),
		( "file", ".txt", "file.txt"),
		( "file.docx", ".docx","file.docx"),
		( "file", ".docx","file.docx"),
		( "file.docx.txt", ".docx",False),
		( "file.pdf", ".pdf","file.pdf"),
		( "file", ".pdf","file.pdf"),
		( "file.pdf.txt", ".pdf",False),
]

@pytest.mark.parametrize("name_obj, setExt, expected", test_data_2)
def test_allowed_file_type_check_obj(name_obj, setExt, expected, s3_botoObj):
	s3_botoObj.setExt(setExt)
	result = s3_botoObj.allowed_file_type(name_obj,"check_obj")
	assert result == expected

































	

























	






















