import requests
from flask import Flask, request, json, jsonify, Response, make_response
from string import Template
from requests.auth import HTTPBasicAuth
from utils import api_utils

app = Flask(__name__)
AIRFLOW_API = 'http://127.0.0.1:8080/api/v1/dags'
auth = HTTPBasicAuth('airflow', 'airflow')


@app.route('/')
@app.route('/health')
def health():
    return make_response(jsonify({"message": f"i am alive"}), 200)


@app.route('/create_dag', methods=['POST', 'GET', 'PUT', 'DELETE'])
def create_dag():
    if request.method == 'POST':
        args = json.loads(request.data)
        try:
            with open('./templates/dag.template', 'r') as reader:
                src_file = Template(reader.read())
        except Exception as ex:
            print(f"Failed opened dag template, error {ex}")
            return make_response(jsonify({"message": "cannot process the request"}), 422)
        result = src_file.substitute({
            "user": args["user"],
            "name": args["name"],
            "cron": args["cron"],
            "dag_id": f'{args["user"]}_{args["name"]}'
        })
        dag_name = f'{args["user"]}_{args["name"]}'
        with open(dag_name + '_' + '.py', 'w') as writer:
            writer.write(result)
    else:
        raise Exception('method POST should be passed')
    return make_response(jsonify({"message": f"DAG has been added: http://192.168.10.45:8080/dags/{dag_name}"}), 201)


@app.route('/delete_dag', methods=['DELETE', 'POST', 'GET', 'PUT'])
def delete_dag():
    if request.method == 'DELETE':
        args = json.loads(request.data)
        keys = ['dag_id', 'owner']
        if all(k in args.keys() for k in keys):
            dag_id = args['dag_id']
            owner = args['owner']
            permanently = args['permanently'] if 'permanently' in args.keys() else False
            response_status, response_content = api_utils.find_dag_by_id(dag_id, AIRFLOW_API, auth)
            if response_status == 200:
                return api_utils.delete_dag_by_id(response_content, owner, auth, dag_id, AIRFLOW_API, permanently)
            else:
                return make_response(jsonify({'message': f'{dag_id} deletion FAILED due to missing dag'}), 404)
        else:
            return make_response(jsonify({'message': 'dag_id and owner params must be pass'}), 404)
    else:
        raise Exception('method DELETE should be passed')


@app.route('/edit_cron', methods=['DELETE', 'POST', 'GET', 'PUT', 'PATCH'])
def edit_cron():
    editable = ['is_paused']
    if request.method == 'PATCH':
        args = json.loads(request.data)
        dag_id = args['dag_id']
        del args['dag_id']
        if len(args) <= 0:
            return make_response(jsonify({'message': 'fsdfeg'}), 500)
        key = [k for k in args.keys() if k != 'dag_id'][0]
        if key in editable:
            data = {key: args[key]}
            rea = requests.patch(AIRFLOW_API+f'/{dag_id}', json=data, auth=auth)
            return make_response(jsonify({'message': 'success'}), 200)
        return make_response(jsonify({'message': 'failed'}), 500)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
