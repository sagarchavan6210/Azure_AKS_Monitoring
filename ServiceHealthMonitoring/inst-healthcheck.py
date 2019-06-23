import requests
import datetime
import json
import yaml
import time
import os,sys
import threading

######################## TO DO ######################################
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
################### Remove this with certfile ###################

index_dict = {
    "nfr" : "/mdsp_azure_nfr/health",
    "prod": "/mdsp_azure_prod/health"
   }

app_list_yml = "main.yaml"
now          = datetime.datetime.now()
data         = {}
ES           = os.getenv("ESBASEURL", "esurl")
JENKINS_URL  = os.getenv("BUILD_URL")
es_data      = {}
threads      = []

## checking ESBASEURL
if ES == "":
	print("unable to fetch ESBASEURL ")
	exit()

mstart = time.time()
try:
	env                 = sys.argv[1]
	tenantid            = sys.argv[2]
	clientid            = sys.argv[3]
	clientsecret        = sys.argv[4]
except:
	raise('Invalid input \n Please run as \'' +sys.argv[0]  + '\' env','tenantid' ,'clientid','clientsecret')

index = index_dict.get(env)
stat  = "/" + now.strftime("%Y%m%d%H%M%S")
ESURL = ES + index + stat

def readDataFromfile():
	data = yaml.load(open(app_list_yml))
	for inc in data.get("includes", []):
		try:
			data.update(yaml.load(open(inc)))
		except Exception as e:
			print (e)
	return data

def getaccesstoken(tenantid,clientid,clientsecret):

	Uri = "https://login.microsoftonline.com/"+ tenantid + "/oauth2/token?api-version=1.0"

	## Getting access token 
	data = {
		'grant_type':'client_credentials',
		'resource':'https://management.core.windows.net/',
		'client_id':clientid,
		'client_secret':clientsecret
		}
	accesstokendata = requests.post(Uri, data=data).content.decode("utf-8")
	return (json.loads(accesstokendata).get('access_token'))
	
def getname(data, name):

	for line in data:
		if name in line:
			return line.split(',')[4]

def health_check(account_name_prefix,env,applist,subid,token):
	
		for servicename, servicedata in applist.items():
			print ("checking Healthy instance for :", account_name_prefix, servicename)
			healthy_instance_count = 0
			total_time             = 0
			total_no_of_inst       = 0

			if servicedata.get('type') == 'aks':
				URL = 'https://' + servicename + '.' + readDataFromfile().get(account_name_prefix).get('domain').get(env) + servicedata.get('healthurl')
				#print ('['+servicename +'] checking health of {}'.format("aks"))
				print ("AKS Service, url :",account_name_prefix, servicename, URL) 
				try:
					raw_data  = requests.get(url=URL, verify=False, timeout=30)

					print ('['+servicename +' RESPONSE] Status code',raw_data.status_code)
					print ('['+servicename +' RESPONSE] Total time' , raw_data.elapsed.total_seconds())
					print ('['+servicename +' RESPONSE] Message', raw_data.content)
					
					if raw_data.status_code == 200:
						healthy_instance_count = 1
						total_time = raw_data.elapsed.total_seconds()
						total_no_of_inst = 1

				except requests.exceptions.HTTPError as errh:
						print ("Http Error:",errh)
				except requests.exceptions.ConnectionError as errc:
					print ("Error Connecting:",errc)
				except requests.exceptions.Timeout as errt:
					print ("Timeout Error:",errt)
				except requests.exceptions.RequestException as err:
					print ("Something Else",err)
				except Exception as e:
					print ('error',e)
					
			elif servicedata.get('type') == 'funct':
				URL = 'https://' + servicename + env +'.azurewebsites.net/api/v3/'+ servicedata.get('healthurl') + '?code='+ readDataFromfile().get(account_name_prefix).get('healthcode').get(env)
				print (URL)
				try:
					raw_data  = requests.get(url=URL, verify=False, timeout=60)
					print ('['+servicename +' RESPONSE] Status code',raw_data.status_code)
					print ('['+servicename +' RESPONSE] Total time' , raw_data.elapsed.total_seconds())
					print ('['+servicename +' RESPONSE] Message', raw_data.content)
					
					if raw_data.status_code == 200:
						healthy_instance_count = 1
						total_time = raw_data.elapsed.total_seconds()
						total_no_of_inst = 1

				except requests.exceptions.HTTPError as errh:
						print ("Http Error:",errh)
				except requests.exceptions.ConnectionError as errc:
					print ("Error Connecting:",errc)
				except requests.exceptions.Timeout as errt:
					print ("Timeout Error:",errt)
				except requests.exceptions.RequestException as err:
					print ("Something Else",err)
				except Exception as e:
					print ('error',e)	
			else:

				URL          = "https://management.azure.com/subscriptions/" +  subid + "/resourceGroups/mdsp-" + account_name_prefix + "-"+ env +"/providers/Microsoft.Web/sites/"+ servicename +"/instances?api-version=2018-02-01"
				headers      = {'Authorization': 'Bearer '+ str(token)}
				raw_data     = requests.get(url = URL, headers=headers).content.decode("utf-8")
				inst_details = json.loads(raw_data).get('value')

				if inst_details == None:
					print ('[WARN] No instance are running of {} {}'.format(account_name_prefix,servicename))
					continue

				total_no_of_inst = len(inst_details)
				print ('[INFO] Number of running instance of {} {} is {}'.format(account_name_prefix,servicename,total_no_of_inst))

				for inst in inst_details:
					try:

						inst_name = inst.get('name')
						URL       = 'https://' + servicename + '.ase.' + account_name_prefix + '-' + env +  readDataFromfile().get('common').get(env) + servicedata.get('healthurl')
						headers   = {'Cookie': "ARRAffinity=" + str(inst_name) }
						print ('['+servicename +'] checking health of {}'.format(inst_name))
						raw_data  = requests.get(url=URL, headers=headers, verify=False, timeout=30)
						
						print ('['+servicename +' RESPONSE] Status code',raw_data.status_code)
						print ('['+servicename +' RESPONSE] Total time' , raw_data.elapsed.total_seconds())
						print ('['+servicename +' RESPONSE] Message', raw_data.content)
						if raw_data.status_code == 200:
							healthy_instance_count = healthy_instance_count + 1
							total_time = total_time + raw_data.elapsed.total_seconds()

					except requests.exceptions.HTTPError as errh:
						print ("Http Error:",errh)
					except requests.exceptions.ConnectionError as errc:
						print ("Error Connecting:",errc)
					except requests.exceptions.Timeout as errt:
						print ("Timeout Error:",errt)
					except requests.exceptions.RequestException as err:
						print ("Something Else",err)
					except Exception as e:
						print ('error',e)

			try:
				avg_total_time = total_time / total_no_of_inst
			except ZeroDivisionError:
				avg_total_time = 0

			es_data['timestamp'] = now.isoformat()
			es_data['JENKINS_URL'] = JENKINS_URL
			es_data[servicename] = {
                            'total_no_of_inst': total_no_of_inst,
                            'healthy_instance': healthy_instance_count ,
                            'time': avg_total_time,
                            'environment_name': env,
                            'product_line': account_name_prefix
							}

			print ('['+servicename + ' INFO] Total no of instance : {}'.format(total_no_of_inst))
			print ('['+servicename + ' INFO] Healthy instance     : {}'.format(healthy_instance_count))
			print ('['+servicename + ' INFO] Time                 : {}'.format(avg_total_time ))
			
			#if healthy_instance_count != total_no_of_inst:
			#    print ("[INSTANCE_ERROR]: Unhelathy instance found in product line",account_name_prefix,"service ",servicename,"healthy_instance_count",healthy_instance_count ,"total total instance count",total_no_of_inst)
				# uncomment this to enable PagerDuty
			    # trigger_pagerduty(servicename,account_name_prefix,healthy_instance_count,total_no_of_inst)
			#else:
			#  	print ("[INSTANCE_INFO]: Helathy instance found in product line {} service {} ".format(account_name_prefix,	servicename))

		
def trigger_pagerduty(servicename,account_name_prefix,healthy_instance_count,inst_details):

	print("[PAGERDUTY INFO] Job triggerd for product line " + account_name_prefix +" service " + servicename )
	BUILD_URL    = os.getenv("BUILD_URL")
	pagerdutykey = os.getenv("pagerdutykey")
	pagerdutyurl = "https://events.pagerduty.com/generic/2010-04-15/create_event.json"
	payload      = {
                  "service_key": pagerdutykey,
                  "event_type": "trigger",
                  "description": "Unhelathy instance found in product line "  + account_name_prefix + " service " + servicename,
                  "client": "Azure Monitoring Service",
                  "client_url": "https://monitoring.service.com",
                  "details": {
                    "total no of inst": inst_details,
                    "healthy instance": healthy_instance_count,
                    "Jenkins job URL": BUILD_URL
                  },
                  "contexts":[ 
                    {
                      "type": "link",
                      "href": "http://mindspherepune.pagerduty.com",
                      "text" : "View in custom tool"
                    },{
                      "type": "link",
                      "href": "http://mindspherepune.pagerduty.com",
                      "text": "View the incident on PagerDuty"
                    }
                  ]
				}
	
	state = requests.post(pagerdutyurl, data=json.dumps(payload))
	print ("[PAGERDUTY INFO] request status code",state.status_code)


def baseUtil(env):

	start = time.time()

	az_data = readDataFromfile()
	
	for account_name_prefix,appdata in az_data.items():
		if account_name_prefix == "common"  or account_name_prefix == "includes":
			continue
		applist = appdata.get('applications')
		subid   = appdata.get('subids').get(env)
		token   = getaccesstoken(tenantid,clientid,clientsecret)
		
		threads.append(threading.Thread(
                                target = health_check,
                                args   = (account_name_prefix,env,applist,subid,token)))
		threads[-1].start()
    
	for thread in threads:
		thread.join()
	
	print (es_data)
	print (ESURL)
	#Pushing es_data to ES
	try:
		state = requests.post(url     = ESURL,
                          data    = json.dumps(es_data),
                          headers = {"Content-Type": "application/json" },
                          timeout = 10
                          )
    
		print ('[ES_INFO] POST STATUS:', state.status_code, state.content)
	except:
		print('[ES_ERROR] Post Error')
    
	end = time.time()
	print ("Total Time", end - start)

baseUtil(env)
