import logging.config
import os.path

_LOGGING_CONFIG = os.path.join(os.path.dirname(__file__), "logging.ini")
logging.config.fileConfig(_LOGGING_CONFIG)

DB_URL = 'mysql+oursql://root:admins@127.0.0.1:3306/cb_sammy'
SERVER_URL = 'http://dawa.aws.dk/'
