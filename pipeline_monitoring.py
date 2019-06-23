#!/usr/bin/env python

__author__  = ''
__version__ = '1.0.0'
__emailid__ = ''
__description__ = "This script is for pipeline monitoring. "

from elasticsearch5 import Elasticsearch, ConnectionError, SSLError, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
import requests
import datetime
import json
import time
import os

mstart         = time.time() # script start time 
now            = datetime.datetime.now()
ES_HOSTNAME    = os.environ['ES_HOSTNAME'] # Elasticsearch hostname
ES_INDEX       = os.environ['ES_INDEX']    # Elasticsearch Index
ES_DOCTYPE     = os.environ['ES_DOCTYPE']  # Elasticsearch doctype
ES_ENDPOINT_ID = os.environ['CI_JOB_ID'] # Elasticsearch endpoint id
ES_ACCESS_KEY  = os.environ['ES_ACCESS_KEY_ID'] # ES_ACCESS_KEY
ES_SECRET_KEY  = os.environ['ES_SECRET_ACCESS_KEY'] # ES_SECRET_KEY
REGION         = os.environ['ES_DEFAULT_REGION'] # ES REGION
ES_DATA        = {}   # Elasticsearch Data

print("ES_HOSTNAME    {}".format(ES_HOSTNAME))
print("ES_INDEX       {}".format(ES_INDEX))
print("ES_DOCTYPE     {}".format(ES_DOCTYPE))
print("ES_ENDPOINT_ID {}".format(ES_ENDPOINT_ID))

def push_data(data):

  try:
    es = Elasticsearch(
                        hosts=[{'host': ES_HOSTNAME, 'port': 443}],
                        http_auth=AWS4Auth(ES_ACCESS_KEY, ES_SECRET_KEY, REGION, 'es'),
                        use_ssl=True,
                        verify_certs=True, 
                        connection_class=RequestsHttpConnection
                        )
    state  = es.index(index=ES_INDEX,doc_type=ES_DOCTYPE, id=ES_ENDPOINT_ID, body=data)
    print ("POST STATUS: {}".format(state))
    return state
  except Exception as e:
      raise e


def mandatorydata(key,val):

	"""  raise error for empty variable """
	try:
		ES_DATA[key] = os.environ[val]
	except KeyError as ke:
		raise ('{} value can\'t be null \n{}'.format(key,ke))

def optdata(key,val):
	
	"""  set None value for empty variable """
	try:
		ES_DATA[key] = os.environ[val]
	except KeyError as ke:
		ES_DATA[key] = ""

def pipelines_data():

	"""  Push pipelines data to Elasticsearch """
	
	## Creating data for ES
	ES_DATA['timestamp'] = now.isoformat()
	ES_DATA['mdsp_pipeline_url'] = os.environ['CI_PROJECT_URL'] + '/-/jobs/' + os.environ['CI_JOB_ID']

	mandatorydata('mdsp_tag_environment','Environment')
	mandatorydata('mdsp_tag_type','app_type')
	mandatorydata('mdsp_sub_group','sub_group')
	mandatorydata('mdsp_products_line','account_name_prefix')
	mandatorydata('mdsp_gitlab_yaml_version','gitlab_version')
	mandatorydata('ci_pipeline_id','CI_PIPELINE_ID')
	mandatorydata('ci_server','CI_SERVER')
	mandatorydata('ci_server_name','CI_SERVER_NAME')
	mandatorydata('ci_job_name','CI_JOB_NAME')
	mandatorydata('ci_runner_id','CI_RUNNER_ID')

	optdata('ci_commit_ref_name','CI_COMMIT_REF_NAME')
	optdata('ci_commit_ref_slug','CI_COMMIT_REF_SLUG')
	optdata('ci_commit_sha','CI_COMMIT_SHA')
	optdata('ci_commit_tag','CI_COMMIT_TAG')
	optdata('ci_config_path','CI_CONFIG_PATH')
	optdata('ci_debug_trace','CI_DEBUG_TRACE')
	optdata('ci_disposable_environment','CI_DISPOSABLE_ENVIRONMENT')
	optdata('ci_environment_name','CI_ENVIRONMENT_NAME')
	optdata('ci_environment_slug','CI_ENVIRONMENT_SLUG')
	optdata('ci_environment_url','CI_ENVIRONMENT_URL')
	optdata('ci_job_id','CI_JOB_ID')
	optdata('ci_job_manual','CI_JOB_MANUAL')
	optdata('ci_job_stage','CI_JOB_STAGE')
	optdata('ci_runner_description','CI_RUNNER_DESCRIPTION')
	optdata('ci_runner_tags','CI_RUNNER_TAGS')
	optdata('ci_pipeline_triggered','CI_PIPELINE_TRIGGERED')
	optdata('ci_pipeline_source','CI_PIPELINE_SOURCE')
	optdata('ci_project_dir','CI_PROJECT_DIR')
	optdata('ci_project_id','CI_PROJECT_ID')
	optdata('ci_project_name','CI_PROJECT_NAME')
	optdata('ci_project_namespace','CI_PROJECT_NAMESPACE')
	optdata('ci_project_path','CI_PROJECT_PATH')
	optdata('ci_project_url','CI_PROJECT_URL')
	optdata('ci_project_visibility','CI_PROJECT_VISIBILITY')
	optdata('ci_registry','CI_REGISTRY')
	optdata('ci_registry_image','CI_REGISTRY_IMAGE')
	optdata('ci_registry_user','CI_REGISTRY_USER')
	optdata('ci_server_revision','CI_SERVER_REVISION')
	optdata('ci_server_version','CI_SERVER_VERSION')
	optdata('ci_shared_environment','CI_SHARED_ENVIRONMENT')
	optdata('artifact_download_attempts','ARTIFACT_DOWNLOAD_ATTEMPTS')
	optdata('get_sources_attempts','GET_SOURCES_ATTEMPTS')
	optdata('restore_cache_attempts','RESTORE_CACHE_ATTEMPTS')

	# print( json.dumps(ES_DATA))
	push_data(ES_DATA) #Pushing data for ES

pipelines_data()

mend = time.time()
