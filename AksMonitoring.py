import sys
import yaml
import glob
import json
import requests
from datetime import datetime
from datetime import timedelta
import logging 
import time

ES = "Your Elasticsearch URL with Index and Doc type" 
now  = datetime.utcnow()
kub_data = {}
node_data = {}
detailed_node_data_cpu = {}
detailed_node_data_memory = {}
start_time, end_time=now - timedelta(seconds=300), str(now.strftime("%Y-%m-%dT%H:%M:%S.000Z"))
start_time=str(start_time.strftime("%Y-%m-%dT%H:%M:%S.000Z"))
kub_data['timestamp'] = now.isoformat() #timestamp for ES Data
stat  = "/" + now.strftime("%Y%m%d%H%M%S")
ESURL = ES  + stat

def get_node_count(headers, aks_url, aks_name): 
	get_node_count_query = {
	"query":"KubeNodeInventory    | where ClusterName == 'aks_name' | distinct  ClusterName, Computer, Status | summarize TotalCount = count(), ReadyCount = sumif(1, Status contains ('Ready')) by ClusterName | extend NotReadyCount = TotalCount - ReadyCount",
	"timespan": "%s/%s"% (start_time, end_time)
	}
	get_node_count = json.dumps(get_node_count_query).replace('aks_name', aks_name)
	number_of_nodes = requests.post(url=aks_url, data = get_node_count, headers=headers)
	data = json.loads(number_of_nodes.content)
	key_list = ['clusterName', 'totalNodeCount', 'readyNodeCount', 'notReadyNodeCount']

	for element in data['tables']:
		for item in element["rows"]:
			print (item)
			value_list = item

	for k, v in zip(key_list, value_list): 
		node_data[k] = v

	kub_data[aks_name]=node_data 

#Get Cluster CPU and Memory 
def get_cluster_cpu_memory(headers, aks_url, aks_name):
	get_cluster_cpu_memory_query = {
	"query": "KubeNodeInventory    | where ClusterName == 'aks_name'  |distinct ClusterName, Computer | join hint.strategy = shuffle(Perf | where ObjectName == 'K8SNode' | where CounterName == 'cpuCapacityNanoCores' or CounterName == 'memoryCapacityBytes' | summarize LimitValue = max(CounterValue) by Computer, CounterName | project Computer, CounterName = iif(CounterName == 'cpuCapacityNanoCores', 'cpu', 'memory'), LimitValue) on Computer | join kind = inner hint.strategy = shuffle(Perf | where ObjectName == 'K8SNode' | where CounterName == 'cpuUsageNanoCores' or CounterName == 'memoryRssBytes' | project Computer, CounterName = iif(CounterName == 'cpuUsageNanoCores', 'cpu', 'memory'), UsageValue = CounterValue) on Computer, CounterName | project ClusterName, Computer, CounterName, UsagePercent = UsageValue * 100.0 / LimitValue | summarize Avg = avg(UsagePercent), P50 = percentiles(UsagePercent, 50, 90, 95) by ClusterName, CounterName",
	"timespan": "%s/%s"% (start_time, end_time)
	}
	get_cluster_cpu_memory = json.dumps(get_cluster_cpu_memory_query).replace('aks_name', aks_name)
	cluster_cpu_memory = requests.post(url=aks_url, data = get_cluster_cpu_memory,headers=headers)
	data = json.loads(cluster_cpu_memory.content)
	key_list = ['clusterName', 'cpuCounter', 'cpuAvg', 'cpuP50', 'cpuP90', 'cpuP95', 'clusterName', 'memCounter', 'memAvg', 'memP50', 'memP90', 'memP95'] 
	value_list = []
	for element in data['tables']:
		for item in element["rows"]:
			for i in item:
				value_list.append(i)

	for k, v in zip(key_list, value_list): 
		node_data[k] = v

	kub_data[aks_name]=node_data

#Get Pod Count 	
def get_pod_count(headers, aks_url, aks_name):
	get_pod_count_query = {
	"query": "KubePodInventory | where ClusterName == 'aks_name' | distinct ClusterName, Computer, PodUid, PodStatus| summarize TotalCount = count(),RunningCount = sumif(1, PodStatus =~ 'Running'), PendingCount = sumif(1, PodStatus =~ 'Pending'), FailedCount = sumif(1, PodStatus =~ 'Failed'),  SucceededCount = sumif(1, PodStatus =~ 'Succeeded')   by ClusterName  | extend UnknownCount = TotalCount - PendingCount - RunningCount - SucceededCount - FailedCount", 
	"timespan": "%s/%s"% (start_time, end_time)

	}
	get_pod_count = json.dumps(get_pod_count_query).replace('aks_name', aks_name) 
	pod_count = requests.post(url=aks_url, data = get_pod_count,headers=headers)
	data = json.loads(pod_count.content)
	key_list = ['clusterName', 'totalPodCount', 'runningPodCount', 'pendingPodCount', 'failedPodCount', 'succeededPodCount', 'unknownPodCount']
	value_list = []
	for element in data['tables']:
		for item in element["rows"]:
			value_list = item
	
	for k, v in zip(key_list, value_list): 
		node_data[k] = v
		
	kub_data[aks_name]=node_data

#Get Node CPU utilization 
def get_node_cpu(headers, aks_url, aks_name):
	get_node_cpu_query = {"query": "KubeNodeInventory    | where ClusterName == 'aks_name' | distinct  ClusterName, Computer| join hint.strategy=shuffle ( Perf |  where ObjectName == 'K8SNode' | where CounterName == 'cpuCapacityNanoCores' | summarize LimitValue = max(CounterValue) by Computer, CounterName  | project Computer, CounterName = iif(CounterName == 'cpuCapacityNanoCores', 'cpu', 'memory'),               LimitValue    ) on Computer | join kind=inner hint.strategy=shuffle ( Perf | where ObjectName == 'K8SNode'        | where CounterName == 'cpuUsageNanoCores' | project Computer, CounterName = iif(CounterName == 'cpuUsageNanoCores', 'cpu', 'memory'), UsageValue = CounterValue ) on Computer, CounterName| project ClusterName, Computer, CounterName, UsagePercent = UsageValue * 100.0 / LimitValue | summarize  Avg = avg(UsagePercent),  P50 = percentiles(UsagePercent, 50, 90, 95) by Computer, CounterName, ClusterName | project ClusterName, Computer, CounterName, Avg, percentile_UsagePercent_95 ",
	"timespan": "%s/%s"% (start_time, end_time)
	}
	get_node_cpu_memory = json.dumps(get_node_cpu_query).replace('aks_name', aks_name)
	nodes = requests.post(url=aks_url, data = get_node_cpu_memory, headers=headers)
	data = json.loads(nodes.content)
	data=data['tables'][0]
	
	key_list = ['clusterName', 'Computer', 'CounterName', 'cpuAvg', 'cpuP95']
	value_dict = {}
	for item in data["rows"]:
		node_details={}
		for k, v in zip(key_list, item):
			node_details[k]=v
		node_name = node_details['Computer']
		detailed_node_data_cpu[node_name] = node_details 

#Get Node Memory utilization 
def get_node_memory(headers, aks_url, aks_name):
	get_node_memory_query = { 
	"query" : "let endDateTime = datetime('%s');" %(str(end_time))  + "let binSize = 5m;  let limitMetricName = 'memoryCapacityBytes';    let usedMetricName = 'memoryWorkingSetBytes';    union (KubeNodeInventory | project ClusterName, ClusterId, Node = Computer, TimeGenerated, Status,              NodeName = Computer, NodeId = strcat(ClusterId, '/', Computer)    | where ClusterName == 'mdsp-advs-dev-aks'        | summarize arg_max(TimeGenerated, Status) by ClusterName, ClusterId, NodeName, NodeId    | join kind=leftouter (        KubeNodeInventory    | project ClusterName, ClusterId, Node = Computer, TimeGenerated, Status,        NodeName = Computer, NodeId = strcat(ClusterId, '/', Computer), Labels        | where ClusterName == 'aks_name'                | summarize arg_max(TimeGenerated, Labels) by ClusterName, ClusterId, NodeName, NodeId    ) on NodeId    | join kind=leftouter (        KubePodInventory  | project ContainerName, NodeId = strcat(ClusterId, '/', Computer)        | distinct NodeId, ContainerName        | summarize ContainerCount = count() by NodeId    ) on NodeId    | join kind=leftouter (        Perf  | where ObjectName == 'K8SNode'        | where CounterName == 'restartTimeEpoch'        | extend NodeId = InstanceName        | summarize arg_max(TimeGenerated, CounterValue) by NodeId        | extend UpTimeMs = datetime_diff('Millisecond', endDateTime,            datetime_add('second', toint(CounterValue), make_datetime(1970,1,1)))        | project NodeId, UpTimeMs    ) on NodeId    | join kind=leftouter (        Perf     | where ObjectName == 'K8SNode'        | where CounterName == limitMetricName        | extend NodeId = InstanceName        | summarize CounterValue = avg(CounterValue) by NodeId        | project NodeId, LimitValue = CounterValue    ) on NodeId    | join kind=leftouter (        Perf    | where ObjectName == 'K8SNode'        | where CounterName == usedMetricName        | extend NodeId = InstanceName        | summarize Aggregation = percentile(CounterValue, 95) by NodeId        | project NodeId, Aggregation    ) on NodeId    | join kind=leftouter (        Perf  | where ObjectName == 'K8SNode'        | where CounterName == usedMetricName        | extend NodeId = InstanceName        | summarize TrendAggregation = percentile(CounterValue, 95) by NodeId, bin(TimeGenerated, binSize)        | project NodeId, TrendAggregation, TrendDateTime = TimeGenerated     ) on NodeId    | project ClusterName, NodeName, LastReceivedDateTime = TimeGenerated, Status, ContainerCount,    UpTimeMs, Aggregation, LimitValue, TrendDateTime, TrendAggregation, Labels),    (KubePodInventory     | where isnotempty(ClusterName)        | where isnotempty(Namespace)        | where isempty(Computer)        | extend Node = Computer        | where ClusterName == 'mdsp-advs-dev-aks'                | where isempty(ContainerStatus)        | where PodStatus == 'Pending'        | order by TimeGenerated desc        | take 1        | project ClusterName, NodeName = 'UnscheduledPods',        LastReceivedDateTime = TimeGenerated, Status = 'unscheduled', ContainerCount = 0, UpTimeMs = '0', Aggregation = '0',        LimitValue = '0', TrendDateTime = TimeGenerated)        | project ClusterName, NodeName, LastReceivedDateTime, Status, ContainerCount, UpTimeMs = UpTimeMs_long,       TrendAggregation,  UsagePercent = Aggregation_real * 100.0 / LimitValue_real| summarize  AvgMemory = avg(UsagePercent),  memoryP50 = percentiles(UsagePercent,50,90,95), avg(TrendAggregation) by  NodeName, Status, ContainerCount, UpTimeMs | project NodeName, Status, ContainerCount, UpTimeMs, AvgMemory, percentile_UsagePercent_90, avg_TrendAggregation", 
	"timespan": "%s/%s"% (start_time, end_time)
	}
	get_node_memory = json.dumps(get_node_memory_query).replace('aks_name', aks_name)
	nodes = requests.post(url=aks_url, data = get_node_memory, headers=headers)
	data = json.loads(nodes.content)
	if nodes.status_code == 200:
		data=data['tables'][0]
	else:
		logger.error("Request get the data node memory from AKS failed.")
		return
	key_list = ['NodeName','Status', 'ContainerCount', 'UpTimeMs', 'memoryAvg', 'memoryP95', 'memoryAvgBytes']
	value_dict = {}
	for item in data["rows"]:	
		node_details={}
		for k, v in zip(key_list, item if item else "Unknown"): 
			if k == "Status" and v == "Ready":
				v = 1
			elif k == "Status" and v == "":
				v = 0
				
			node_details[k]=v
		node_name = node_details['NodeName']
		detailed_node_data_memory[node_name] = node_details

#Merge both cpu and memory node data into single JSON
def merge_node_cpu_memory():
	node_data_cm = {}
	for key1, value1 in detailed_node_data_cpu.items():
		node_data_cm[key1]=value1
		for key2, value2 in detailed_node_data_memory.items():
			if key2 == "UnscheduledPods":
				pass
			elif key1 == key2:
				node_data_cm[key1].update(value2)
	for name, data in node_data_cm.items():
		kub_data[name]=data

#Push data to ES
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

#Authenticate and call all funtions	
def getdata(resource_group, tenant_id, client_id, client_secret, subscription_id):
	URI = "https://login.microsoftonline.com/" + tenant_id + "/oauth2/token?api-version=1.0"
	data = {
		'grant_type': 'client_credentials',
        'resource': 'https://management.core.windows.net/',
        'client_id': client_id,
        'client_secret': client_secret
    }
	accesstokendata = requests.post(URI, data=data).content
	accesstoken = (json.loads(accesstokendata).get('access_token'))
	headers = {'Authorization': 'Bearer ' + str(accesstoken), 'content-type': "application/json"}
	aks_name = resource_group +"-aks"

	aks_url = "https://management.azure.com/subscriptions/"+ subscription_id + "/resourcegroups/" + resource_group + "/providers/microsoft.operationalinsights/workspaces/" + resource_group +"-aks-log-analytics-workspace/query?api-version=2017-10-01"
	
	get_node_count(headers, aks_url, aks_name)
	get_cluster_cpu_memory(headers, aks_url, aks_name)
	get_pod_count(headers, aks_url, aks_name)
	get_node_cpu(headers, aks_url, aks_name)
	get_node_memory(headers, aks_url, aks_name)
	merge_node_cpu_memory()
	post_data_to_es()

#Main Funtion		
if __name__ == "__main__":
	try:
		env                 = sys.argv[1]
		account_name_prefix = sys.argv[2]
		tenant_id           = sys.argv[3]
		client_id           = sys.argv[4]
		client_secret       = sys.argv[5]
		subscription_id		= sys.argv[6]
	except:
		raise('Invalid input \n Please run as \'' +sys.argv[0]  + '\' env','tenantid' ,'clientid','clientsecret')
		
	resource_group = "Your Resource Group Name"
	print (resource_group)
	getdata(resource_group, tenant_id, client_id, client_secret, subscription_id)
