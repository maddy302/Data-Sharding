import requests

endPoint = ['http://localhost:5000','http://localhost:5001','http://localhost:5002','http://localhost:5003']
api_end_point = '/api/v1/entries'

for x in endPoint:
    requests.delete(x+api_end_point)