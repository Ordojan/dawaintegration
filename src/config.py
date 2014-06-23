import logging.config
import os.path

_LOGGING_CONFIG = os.path.join(os.path.dirname(__file__), "logging.ini")
logging.config.fileConfig(_LOGGING_CONFIG)

DB_URL = 'mysql+oursql://root:admin@127.0.0.1:1521/SAMMY'
SERVER_URL = 'http://dawa.aws.dk/'
