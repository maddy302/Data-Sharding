from flask import jsonify
import hashlib
import bisect
from collections import defaultdict
import binascii
import time
import pandas
from functools import wraps
import requests

class DeathRecord:
    year = 0
    cause_name = ""
    cause = ""
    deaths = 0
    state = ""
    death_rate = ""


# Consistent Hashing - 
# When the class is called, it will create hashes for each server available. If there are only 5 servers, there may arise hotspots.
# To mitiage and maintain uniform distribution, virutal nodes are created (replicas)
# Each server will be responsible for certain nodes on a ring, not contiguous though
# 
class ConsistentHahsing:

    def __init__(self, replicas = 10):
        self.servers = ['http://localhost:5000','http://localhost:5001','http://localhost:5002','http://localhost:5003']
        self.end_point = '/api/v1/entries'
        self.nb_nodes = len(self.servers)
        self.replicas = replicas
        #Master dictionary that will contain all the mappings of the hash of nodes vs the server ip
        self.server_node_mappings = {}

        for x in self.servers:
            self.__map_replicas_nodes(x)
        self.node_list = list(self.server_node_mappings.keys())
        self.node_list.sort()

    #A utility hash function, used md5 hashing.
    def __hash(self, key):
        temp = hashlib.md5(key.encode()).hexdigest()
        return int(temp,16)

    #Generate n virtual nodes and hashes 
    def __giveReplicasForNodes(self, node):
        replica_hash_list = list()
        for i in range(self.replicas):
            replica_hash_list.append(self.__hash(node+':'+str(i)))
        return replica_hash_list

    #Map the hash of the virtual node to the appropriate server
    def __map_replicas_nodes(self, node):
        for hash in self.__giveReplicasForNodes(node):
            self.server_node_mappings[hash] = node

    #Given the key of the data, it will hash and give the IP of the server on the ring to store the data
    def __get_node_for_key(self, key):
        hash = self.__hash(key)
        st = bisect.bisect(self.node_list, hash)
        if st == len(self.node_list):
            st = 0
        #print(self.server_node_mappings[self.node_list[st]])
        return self.server_node_mappings[self.node_list[st]]

    def read_file(self):
        data = pandas.read_csv('csv_file.csv')
        count = 0
        for i in range(len(data['Year'])):
            #form key for the data
            key = str(data['Year'][i]) + data['Cause Name'][i] + data['State'][i]
            data_store = self.__get_node_for_key(key)
            #print(data_store)
            data_temp = {
                'Year' : str(data['Year'][i]),
                'Cause' : data['113 Cause Name'][i],
                'Cause Name': data['Cause Name'][i],
                'State' : data['State'][i],
                'Deaths' : str(data['Deaths'][i]),
                'Death Rate' : str(data['Age-adjusted Death Rate'][i])
            }
            
            data_to_post = {self.__hash(key) : data_temp}
            data_store = data_store+self.end_point
            res = requests.post(data_store,json = data_to_post)
            #print(res)
            if(res.status_code == 201):
                count = count + 1
        print("Uploaded "+str(count) + " entries.")

    def get_entries(self):
        print("Verifying the data.")
        for x in self.servers:
            res = requests.get(x+self.end_point)
            data_json = res.json()
            print("\nGET "+x)
            print(data_json)
        
if __name__ == '__main__':
    cr = ConsistentHahsing(100)
    cr.read_file()
    cr.get_entries()

