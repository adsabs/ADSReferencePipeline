
# -*- coding: utf-8 -*-

LOG_STDOUT = False

REFERENCE_PIPELINE_ADSWS_API_TOKEN = 'this is a secret api token!'
REFERENCE_PIPELINE_SERVICE_TEXT_URL = 'https://dev.adsabs.harvard.edu/v1/reference/text'
REFERENCE_PIPELINE_SERVICE_XML_URL = 'https://dev.adsabs.harvard.edu/v1/reference/xml'

REFERENCE_PIPELINE_MAX_NUM_REFERENCES = 16

# db config
SQLALCHEMY_URL = 'postgresql+psycopg2://postgres:postgres@localhost:5432/postgres'
SQLALCHEMY_TRACK_MODIFICATIONS = False
SQLALCHEMY_ECHO = False


# possible values: WARN, INFO, DEBUG
LOGGING_LEVEL = 'DEBUG'


# celery config
CELERY_INCLUDE = ['adsrefpipe.tasks']
CELERY_BROKER = 'pyamqp://'
# for result backend
REDIS_BACKEND = "redis://localhost:6379/0"


# if task did not finish in this many seconds, abort
TASK_PROCESS_TIME = 30
# checking queues every this many seconds
QUEUE_AUDIT_INTERVAL = 10

# true if to compare the resolved records with classic
COMPARE_CLASSIC = False
