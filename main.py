from flask import Flask, request
from flasgger import Swagger
import aerospike
from pymongo import MongoClient
from google.cloud.bigquery_storage_v1 import types
from google.cloud import bigquery_storage_v1
import os

app = Flask(__name__)
swagger = Swagger(app)

class Aerospike():
    def __init__(self):
        host_port = [("host1", 3000), ("host2", 3000)]
        config = {
            "hosts": host_port,
            "policies": {
            "timeout": 20000
            }
        }
        self.aero_client = aerospike.client(config)
        self.aero_client.connect()

    def query_aero(self, user_id):
        key = ('namespace_name', 'set_name', user_id)
        try:
            _, _, record = self.aero_client.get(key)
            return record['bin']
        except:
            return 'cannot user find in aerospike'

class Mongo():
    def __init__(self):
        client = MongoClient('host3')
        self.db = client['database_name']
        
    def scan_mongo(self):
        segments = self.db.collection_name.find({}, {'name':1})
        seg_dict = {}
        for segment in segments:
            seg_dict[segment['id']] = segment['name']
        return seg_dict

def read_table():
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] ='service_account.json'
    bqstorageclient = bigquery_storage_v1.BigQueryReadClient()

    project_id = "project_id"
    dataset_id = "dataset_id"
    table_id = "table_id"
    table = f"projects/{project_id}/datasets/{dataset_id}/tables/{table_id}"

    read_options = types.ReadSession.TableReadOptions(
        selected_fields=["field1", "field2"]
    )

    parent = "projects/{}".format(project_id)

    requested_session = types.ReadSession(
        table=table,
        data_format=types.DataFormat.ARROW,
        read_options=read_options,
    )
    read_session = bqstorageclient.create_read_session(
        parent=parent,
        read_session=requested_session
    )

    stream = read_session.streams[0]
    reader = bqstorageclient.read_rows(stream.name)

    rows = reader.rows(read_session)
    dict = {}
    for row in rows:
        dict[str(row['field1'])] = str(row['field2'])
    return dict

aero = Aerospike()
mongo = Mongo()
seg_dict = mongo.scan_mongo()
user_mapping = read_table()

@app.route('/')
def find_segment():
    """return a dictionary of user profile
    ---
    parameters:
      - name: id
        in: query
        type: string
        required: true
    """

    user_id = request.args.get('id').strip()
    user_segment_dict = aero.query_aero(user_id)
    output = {user_id: {}}
    for seg in user_segment_dict:
        output[user_id][seg] = seg_dict[seg]

    if user_id in user_mapping:
        mapping_id = user_mapping[user_id]
        output[mapping_id] = {}
        user_segment_dict = aero.query_aero(mapping_id)
        for seg in user_segment_dict:
            output[mapping_id][seg] = seg_dict[seg]

    return output

if __name__ == '__main__':
    app.run(port=7000)
