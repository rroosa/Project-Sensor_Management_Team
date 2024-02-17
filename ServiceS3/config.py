class Config(object): 
	DEBUG = False
	TESTING = False
	PORT = 5002  # port di default
	UPLOAD_FOLDER = 'static/upload'
	DOWNLOAD_FOLDER = 'static/download'


class ProductionConfig(Config):
	DEBUG = False

class DevelopmentConfig(Config):
	DEBUG = True

class TestingConfig(Config):
	TESTING = True
	GROUP_NAME = "SensorsManagerTeam"
	SESSION_NAME = "AssumeRoleSession"

	BUCKET_DATA_SHEET = "sensors-data-sheet"
	BUCKET_TEMAPLATE = "sensor-device-template"

	

