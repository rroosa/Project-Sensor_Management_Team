class Config(object):
	DEBUG = False
	TESTING = False
	PORT = 5003	# port di default

	TEMPLATE_DEVICE_FOLDER = 'static/template_device' 
	DATASET_DEVICE_FOLDER = 'static/dataset_device' 
	DOWNLOAD_FOLDER = 'static/download' 
	# save csv  file, of query result 


class ProductionConfig(Config):
	DEBUG = False

class DevelopmentConfig(Config):
	DEBUG = True

class TestingConfig(Config):
	TESTING = True
	GROUP_NAME = "SensorsManagerTeam"
	SESSION_NAME = "AssumeRoleSession"


