import requests
import json
from pathlib import Path
import argparse
from datetime import datetime
######################## TO DO ######################################
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
################### Remove this with certfile ###################



parser = argparse.ArgumentParser()
parser.add_argument('--jenkins_user', '-u', help='User Name', required=True)
parser.add_argument('--jenkins_pass', '-p', help='Password', required=True)
args = parser.parse_args()
ES = "esurl:9200/mdsp-az-jenkins/jen_data"
now  = datetime.utcnow()
stat  = "/" + now.strftime("%Y%m%d%H%M%S")
ESURL = ES  + stat

Jenkins_URL        = "jenkinsurl" #os.getenv("Jenkins_URL")
jenkinsusername    = args.jenkins_user
jenkinsbasicpass   = args.jenkins_pass
api = "/computer/api/json"
jenkins 		  = {}
jenkins['timestamp'] = now.isoformat()
az_accounts_path  = Path(__file__).parent / 'config' / 'az_accounts.json'
subscription_data = json.load(open(str(az_accounts_path), 'r'))
 
class jenkinsData:
	def getdatafromjenkins(obj, node_name):
		if node_name == 'master':
			jenkins_url = Jenkins_URL + api
		else:
			jenkins_url = Jenkins_URL +"/computer/"+node_name+ "/api/json"
		try:
			return requests.get(url=jenkins_url,auth=(jenkinsusername, jenkinsbasicpass),verify=False).content
		except requests.exceptions.ProxyError as arpe:
			raise arpe
		except requests.exceptions.HTTPError as errh:
			raise (errh)
		except requests.exceptions.ConnectionError as errc:
			raise (errc)
		except requests.exceptions.Timeout as errt:
			raise ("Timeout Error:",errt)
		except requests.exceptions.RequestException as err:
			print ("Something Else",err)
		except Exception as e:
			raise (e)
	
	def jenkinsStatus(obj, jenkinsdata, node_name):
		for node in jenkinsdata:
			if node['displayName'] == node_name:
				node_stat = node		
		try:
			status = 0 if node_stat['offline'] else 1
		except Exception as e:
			status = 0
		return status
	
	def getJenkinsHealth(obj, jendata, node_name, status):
		availRam = totalRam = diskSize = 0
		diskPath = "" #None
		if status == 1:	
			for key , value in jendata.items():
				if key == 'hudson.node_monitors.SwapSpaceMonitor' or key == 'hudson.node_monitors.DiskSpaceMonitor':
					for k, v in value.items():
						if k == 'availablePhysicalMemory':
							availRam = v
						elif k == 'totalPhysicalMemory':
							totalRam = v
						elif k == 'size':
							diskSize = v
						elif k == 'path':
							diskPath = v
						
		jenkins[node_name] = {
				"status" : status, 
				"availRam" : availRam,
				"totalRam" : totalRam,
				"diskPath" : diskPath,
				"diskSize" : diskSize
		}			
	
	def post_data_to_es(obj):
		print(jenkins)
		try:
			state = requests.post(
							url     = ESURL,
							data    = json.dumps(jenkins),
							headers = {"Content-Type": "application/json" },
							timeout = 10
							)
			print ('[ES_INFO] POST STATUS:', state.status_code, state.content)
		except:
			print('[ES_ERROR] Post Error')

		print (ESURL)

if __name__ == "__main__":
	j = jenkinsData()
	masterdata = {}
	for sub_data in  subscription_data.get("az_accounts"):
		node_name = sub_data['environment'].lower()
		print (node_name)
		if node_name == 'devops-poc':
			pass
		elif node_name == 'master':
			raw_data         = j.getdatafromjenkins(node_name)
			jenkinsdata      = json.loads(raw_data).get('computer')
			status = j.jenkinsStatus(jenkinsdata, node_name)
			for k in jenkinsdata:
				if k['displayName'] == 'master':
					masterdata = k['monitorData']
					j.getJenkinsHealth(masterdata, node_name, status)
		else:
			raw_data         = j.getdatafromjenkins(node_name)
			jenkinstatus     = json.loads(raw_data).get('offline')
			status = 0 if jenkinstatus else 1
			jenkinsdata      = json.loads(raw_data).get('monitorData')	
			j.getJenkinsHealth(jenkinsdata, node_name, status)
	j.post_data_to_es()
