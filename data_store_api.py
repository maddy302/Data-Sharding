from flask import Flask, jsonify, request, abort, make_response
from flask_restful import Resource, Api
import sys

app = Flask(__name__)
# api = Api(app)
endpoint = '/api/v1/entries'
data_list = list()
#data_list = {}


# class crud(Resource):
#     def post(self):
#         data_list.append(request.form['data'])
#         return 201
    
#     def get(self):
#         result = {}
#         if len(data_list) == 0:
#             abort(404)
#         result["num_entries"] = len(data_list)
#         result["entries"] = data_list
#         return jsonify(result), 200

#     def delete(self):
#         data_list.clear()
#         return jsonify({'message' : 'user list cleared'})


class DeathRecord:
    year = 0
    cause_name = ""
    cause = ""
    deaths = 0
    state = ""
    death_rate = ""

    def __init__(self, year, cause_name, cause, deaths, state, death_rate):
        self.year = year
        self.cause_name = cause_name
        self.cause= cause
        self.deaths = deaths
        self.state = state
        self.death_rate = death_rate

#The post endpoint when given the data in the format {<key>:<value>}, will be stored in the global list
@app.route(endpoint, methods = ['POST'])
def insert_record():
    print(request.json)

    if not request.json:
        abort(400)
    data_list.append(request.json)

    return jsonify({'result':'data inserted'}),201


#The get endpoint to return the entire list and the number of entries in a dictionary
@app.route(endpoint, methods = ['GET'])
def get_records():
    return jsonify(
        {"num_entries" : len(data_list),
        'data':data_list
        })


#Error handling, any bad request will return 404
@app.errorhandler(404)
def not_found(error):
    return make_response(jsonify({'error': 'Not found'}), 404)

#get request to post directly via url and get the data for a given key - not in requirements to complete
@app.route(endpoint+'/<string:test_data>', methods = ['GET', 'POST'])
def send_response(test_data):
    return jsonify({'test_data':'passed',
                    'data' : test_data}),200

#Utility method delete to refesh the data in the datastore, only used for easier testing purpose, not in the requirements.
@app.route(endpoint, methods=['DELETE'])
def delete_all_data():
    data_list.clear()
    return jsonify({'result':'data_deleted'})

# api.add_resource(crud, '/api/v1/entries')



if __name__ == '__main__':
    port_nb = sys.argv[1]
    app.run(debug=True, port=port_nb)