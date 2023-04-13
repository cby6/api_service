from flask import Flask, request, render_template, session, url_for, redirect
from flasgger import Swagger
import aerospike
from pymongo import MongoClient
from google.cloud.bigquery_storage_v1 import types
from google.cloud import bigquery_storage_v1
import os
import secrets

app = Flask(__name__)
app.secret_key = secrets.token_bytes(32)
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
            return None

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

def find_segment(user_id):
    """return a dictionary of user profile
    ---
    parameters:
      - name: id
        in: query
        type: string
        required: true
    """

    validate_user_id(user_id)
    user_segment_dict = aero.query_aero(user_id)
    if user_segment_dict is None:
        return f'cannot find associated segments for user {user_id}'
    
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

def validate_user_id(user_id):
    user_id = str(user_id).strip()
    if len(user_id) > 100:
        raise ValueError('user id is too long')
    if len(user_id) < 5:
        raise ValueError('user id is too short')


@app.route('/query_user', methods=['GET', 'POST'])
def query_user():
    if request.method == 'POST':
        user_id = request.form['user_id']
        session['user_id'] = user_id
        return find_segment(user_id)
    return render_template('query_user.html') 

@app.route('/')
def main():
    return find_segment(request.args.get('id'))
            

if __name__ == '__main__':
    app.run(port=7000)
