import sys
import yaml
import glob
import json
import requests
import datetime

ES = "Your Elasticsearch URL with Index and Doc type" 
now  = datetime.datetime.utcnow() //for UTC timestamp

kub_data = {}
node_data = {}
#kub_data.append()
kub_data['timestamp'] = now.isoformat()
stat  = "/" + now.strftime("%Y%m%d%H%M%S")
ESURL = ES  + stat

def get_node_count(headers, aks_url): 
	get_node_count = {
	"query":"KubeNodeInventory    | where ClusterName == 'Your Cluster AKS Name' | distinct  ClusterName, Computer, Status | summarize TotalCount = count(), ReadyCount = sumif(1, Status contains ('Ready')) by ClusterName | extend NotReadyCount = TotalCount - ReadyCount"
	}
	number_of_nodes = requests.post(url=aks_url, data = json.dumps(get_node_count),headers=headers)
	data = json.loads(number_of_nodes.content)
	key_list = ['clusterName', 'totalNodeCount', 'readyNodeCount', 'notReadyNodeCount']
	
	for element in data['tables']:
		for item in element["rows"]:
			value_list = item

	for k, v in zip(key_list, value_list): 

		node_data[k] = v

	kub_data['Your Cluster AKS Name']=node_data


def get_cluster_cpu_memory(headers, aks_url):
	get_cluster_cpu_memory = {
	"query": "KubeNodeInventory    | where ClusterName == 'Your Cluster AKS Name'  |distinct ClusterName, Computer | join hint.strategy = shuffle(Perf | where ObjectName == 'K8SNode' | where CounterName == 'cpuCapacityNanoCores' or CounterName == 'memoryCapacityBytes' | summarize LimitValue = max(CounterValue) by Computer, CounterName | project Computer, CounterName = iif(CounterName == 'cpuCapacityNanoCores', 'cpu', 'memory'), LimitValue) on Computer | join kind = inner hint.strategy = shuffle(Perf | where ObjectName == 'K8SNode' | where CounterName == 'cpuUsageNanoCores' or CounterName == 'memoryRssBytes' | project Computer, CounterName = iif(CounterName == 'cpuUsageNanoCores', 'cpu', 'memory'), UsageValue = CounterValue) on Computer, CounterName | project ClusterName, Computer, CounterName, UsagePercent = UsageValue * 100.0 / LimitValue | summarize Avg = avg(UsagePercent), P50 = percentiles(UsagePercent, 50, 90, 95) by ClusterName, CounterName"
	}
	cluster_cpu_memory = requests.post(url=aks_url, data = json.dumps(get_cluster_cpu_memory),headers=headers)
	data = json.loads(cluster_cpu_memory.content)
	key_list = ['clusterName', 'cpuCounter', 'cpuAvg', 'cpuP50', 'cpuP90', 'cpuP95', 'clusterName', 'memCounter', 'memAvg', 'memP50', 'memP90', 'memP95'] 
	value_list = []
	for element in data['tables']:
		for item in element["rows"]:
			for i in item:
				value_list.append(i)

	for k, v in zip(key_list, value_list): 

		node_data[k] = v

	kub_data['Your Cluster AKS Name']=node_data

	
	
def get_pod_count(headers, aks_url):
	get_pod_count_node_status = {
	"query": "KubeNodeInventory | project ClusterName, ClusterId, Node = Computer, TimeGenerated, Status, NodeName = Computer, NodeId = strcat(ClusterId, '/', Computer) | where ClusterName == 'Your Cluster AKS  Name' | summarize max(Status) by ClusterName, NodeName | join kind = leftouter(KubePodInventory | project ContainerName, NodeName = Computer | distinct NodeName, ContainerName | summarize ContainerCount = count() by NodeName) on NodeName "
	}

	get_pod_count = {
	"query":"KubePodInventory       | where ClusterName == 'Your Cluster AKS Name'  | distinct ClusterName, Computer, PodUid, PodStatus| summarize TotalCount = count(),RunningCount = sumif(1, PodStatus =~ 'Running'), PendingCount = sumif(1, PodStatus =~ 'Pending'), FailedCount = sumif(1, PodStatus =~ 'Failed'),  SucceededCount = sumif(1, PodStatus =~ 'Succeeded')   by ClusterName  | extend UnknownCount = TotalCount - PendingCount - RunningCount - SucceededCount - FailedCount", "timespan": "2019-02-12T06:00:00.000Z/2019-02-12T12:02:18.761Z"
	}
	
	pod_count = requests.post(url=aks_url, data = json.dumps(get_pod_count),headers=headers)
	data = json.loads(pod_count.content)
	key_list = ['clusterName', 'totalPodCount', 'runningCount', 'pendingCount', 'failedCount', 'succeededCount', 'unknownCount']
	value_list = []
	for element in data['tables']:
		for item in element["rows"]:
			value_list = item
	

	for k, v in zip(key_list, value_list): 

		node_data[k] = v
	kub_data['Your Cluster AKS Name']=node_data
	
	
def post_data_to_es():
	print(kub_data)
	try:
		state = requests.post(
							url     = ESURL,
							data    = json.dumps(kub_data),
							headers = {"Content-Type": "application/json" },
							timeout = 10
							)
		print ('[ES_INFO] POST STATUS:', state.status_code, state.content)
	except:
		print('[ES_ERROR] Post Error')

	print (ESURL)
	
	

def getdata(filename):
	with open(filename, 'r') as ymlfile:
		cfg = yaml.load(ymlfile) 

    ## Getting access token
	URI = "https://login.microsoftonline.com/" + cfg['tenant_id'] + "/oauth2/token?api-version=1.0"
	data = {
		'grant_type': 'client_credentials',
        'resource': 'https://management.core.windows.net/',
        'client_id': cfg['client_id'],
        'client_secret': cfg['client_secret']
    }
	accesstokendata = requests.post(URI, data=data).content
	accesstoken = (json.loads(accesstokendata).get('access_token'))
	headers = {'Authorization': 'Bearer ' + str(accesstoken), 'content-type': "application/json" }
	aks_url = "https://management.azure.com/subscriptions/"+cfg['subscription_id']+"/resourcegroups/" + resource_group + "/providers/microsoft.operationalinsights/workspaces/[AKSName-log-analytics-workspace] /query?api-version=2017-10-01"
	get_node_count(headers, aks_url)
	get_cluster_cpu_memory(headers, aks_url)
	get_pod_count(headers, aks_url)
	post_data_to_es()
	
	
if __name__ == "__main__":
	resource_group = "Your Resource Group"
	for filename in glob.glob("config.yml"):
		print(filename)
		print("---------------------------")
		getdata(filename)  