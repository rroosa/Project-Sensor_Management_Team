class Config(object): 
	DEBUG = False
	TESTING = False

	UPLOAD_FOLDER = 'static/upload'
	DOWNLOAD_FOLDER = 'static/download'
	PORT = 5001		 # port di default
	PORT_S3 = "5002" # port di default
	PORT_DB = "5003" # port di default

class ProductionConfig(Config):
	DEBUG = False

class DevelopmentConfig(Config):
	DEBUG = True

class TestingConfig(Config):
	TESTING = True
	BUCKET_TEMPLATE = 'sensor-device-template'
	BUCKET_DATA_SHEET = 'sensors-data-sheet'

	HOST_S3 = f"http://localhost"
	HOST_DB = f"http://localhost"
	







