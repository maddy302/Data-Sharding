import hashlib
import pandas
import requests

# Rendezvous Hashing, the server idetifier is added to the key 
# and the greatest hash formed among all the servers is used to store the data.


class RendezvousHashing:

    def __init__(self):
        self.servers = ['http://localhost:5000','http://localhost:5001','http://localhost:5002','http://localhost:5003']
        self.end_point = '/api/v1/entries'

    #Utility function to generate hash - sha256 is being used
    def __hash(self, key):
        temp = hashlib.sha256(key.encode()).hexdigest()
        return int(temp,16)

    #Given a key , node will be selected
    def __get_node_for_key(self, key):
        high = -1
        server = None
        for x in self.servers:
            temp_hash = self.__hash( key+ x)
            if temp_hash > high:
                high = temp_hash
                server = x
            elif temp_hash == high:
                high, server = temp_hash, max(x, server)

        return server

    #Using pandas lib to read the excel file, forming post requests to store the data, and get requests to display the data
    def read_file(self):
        count = 0
        data = pandas.read_csv('csv_file.csv')
        for i in range(len(data['Year'])):
            key = str(data['Year'][i]) + data['Cause Name'][i]+ data['State'][i]
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
            # data_to_post = {
            #     'data' : data_temp,
            #     'key' : self.__hash(key)
            # }
            data_to_post = {self.__hash(key) :data_temp }

            data_store = data_store + self.end_point
            res = requests.post(data_store, json = data_to_post)
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
    rh = RendezvousHashing()
    rh.read_file()
    rh.get_entries()