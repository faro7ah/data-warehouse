import configparser
import os


_config = configparser.ConfigParser()
_config.read(os.path.join(os.getcwd(), 'sparkify.cfg'))

# ------------- #
# AWS constants #
# ------------- #

AWS_REGION = _config['AWS']['REGION']
AWS_ACCESS_KEY_ID = _config['AWS']['ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = _config['AWS']['SECRET_ACCESS_KEY']

# ------------------ #
# Redshift constants #
# ------------------ #

REDSHIFT_CLUSTER_IDENTIFIER = _config['REDSHIFT']['CLUSTER_IDENTIFIER']
REDSHIFT_CLUSTER_TYPE = _config['REDSHIFT']['CLUSTER_TYPE']
REDSHIFT_NODE_TYPE = _config['REDSHIFT']['NODE_TYPE']
REDSHIFT_NUMBER_OF_NODES = _config['REDSHIFT']['NUMBER_OF_NODES']
REDSHIFT_DB_NAME = _config['REDSHIFT']['DB_NAME']
REDSHIFT_PORT = _config['REDSHIFT']['PORT']
REDSHIFT_MASTER_USERNAME = _config['REDSHIFT']['MASTER_USERNAME']
REDSHIFT_MASTER_USER_PASSWORD = _config['REDSHIFT']['MASTER_USER_PASSWORD']
REDSHIFT_ENDPOINT_ADDRESS = _config['REDSHIFT']['ENDPOINT_ADDRESS']

# ------------- #
# IAM constants #
# ------------- #

IAM_ROLE_NAME = _config['IAM']['ROLE_NAME']

# ------------ #
# S3 constants #
# ------------ #

S3_LOG_DATA = _config['S3']['LOG_DATA']
S3_LOG_JSON_PATH = _config['S3']['LOG_JSON_PATH']
S3_SONG_DATA = _config['S3']['SONG_DATA']

# ------------------------ #
# CloudFormation constants #
# ------------------------ #

CLOUDFORMATION_STACK_NAME = _config['CLOUDFORMATION']['STACK_NAME']

# ------------------ #
# Sparkify constants #
# ------------------ #

SPARKIFYDB_DSN = 'host={} port={} dbname={} user={} password={}'.format(
    REDSHIFT_ENDPOINT_ADDRESS,
    REDSHIFT_PORT,
    REDSHIFT_DB_NAME,
    REDSHIFT_MASTER_USERNAME,
    REDSHIFT_MASTER_USER_PASSWORD
)
