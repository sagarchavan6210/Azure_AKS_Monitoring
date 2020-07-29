import os,sys
import requests
from pprint import pprint
from requests.auth import HTTPBasicAuth
import json
import certifi
import datetime
import time
from requests.exceptions import ProxyError


url = "https://hbaseurl/"
username = "msuser1"
password = ""
Clusters = []
Hosts = []

#ES Data
env   = os.getenv("branch", "prod")
now   = datetime.datetime.utcnow()
ES    = os.getenv("ESBASEURL", "esurl:9200")
index = "/mdsp-hbase-test/hbase_data"
stat  = "/" + now.strftime("%Y%m%d%H%M%S")
ESURL = ES + index + stat
data  = {}

def getclustername():
    clusterurl = url + 'api/v1/clusters/'
    r = requests.get(url=clusterurl, auth=HTTPBasicAuth(username, password),headers ={"Content-Type": "application/json" } ).content.decode("utf-8")
    response = json.loads(r)
    for object in response:
        if(object == 'items'):
            value = response[object]
            for item in value:
                cluster = (item['Clusters'])
                cluster_name = cluster['cluster_name']
                Clusters.append(cluster_name)
    pass

def getHostName():
    hostsurl = url + 'api/v1/hosts'
    r = requests.get(url=hostsurl, auth=HTTPBasicAuth(username, password),headers ={"Content-Type": "application/json" } ).content.decode("utf-8")
    response = json.loads(r)
    for object in response:
        if(object == 'items'):
            value = response[object]
            for item in value:
                hosts = (item['Hosts'])
                host_name = hosts['host_name'] 
                Hosts.append(host_name)
                
    pass

def getclusterdetails():
    baseurl = url + 'api/v1/clusters/' + Clusters[0]
    r = requests.get(url=baseurl, auth=HTTPBasicAuth(username, password),headers ={"Content-Type": "application/json" } ).content.decode("utf-8")
    response = json.loads(r)
    for object in response:
        if(object == 'Clusters'):
            value = response[object]
            health_report = value['health_report']
            data['heathy_hosts'] = health_report['Host/host_state/HEALTHY']
            data['unhealthy_hosts'] = health_report['Host/host_state/UNHEALTHY']
            
    pass

def gethostsdetails():
    baseturl = url + 'api/v1/clusters/'+Clusters[0]+'/hosts/'
    for item in Hosts:
        hosturl = baseturl + item
        r = requests.get(url=hosturl, auth=HTTPBasicAuth(username, password),headers ={"Content-Type": "application/json" } ).content.decode("utf-8")
        response = json.loads(r)
        for object in response:
            if(object == 'Hosts'):
                value = response[object]
                disk_info = value['disk_info']
                tempdata = {}
                for disk_stat in disk_info:
                    percent = disk_stat['percent']
                    if(disk_stat['mountpoint']== '/'):
                        temp_mount_point = 'root'
                    else:
                        temp_mount_point = disk_stat['mountpoint']
                        temp_mount_point = temp_mount_point.strip('/')
                    # mount_point = disk_stat['mountpoint']
                    tempdata.update({temp_mount_point:percent.strip('%')})
                item = item[0:3]
                data[item] = tempdata
    pprint(data)
    pass

def insertdata():
    try:
        state = requests.post(url=ESURL, data = json.dumps(data),headers ={"Content-Type": "application/json" } )
        if state.status_code == 201:
            print ("POST STATUS:", state.status_code, state.content)
            print ("Data Pushed Successfully")
            print(ESURL)
        else:
            print('Status: ' + state.status_code)
    except Exception as e:
        print(e)
    pass



if __name__ == '__main__':
    data['timestamp'] = now.isoformat()
    getclustername()
    getHostName()
    getclusterdetails()
    gethostsdetails()
    insertdata()
