from typing import Tuple, Any, Union
import requests
import json

from flask import make_response, jsonify, Response
from requests.auth import HTTPBasicAuth


def find_dag_by_id(dag_id: str, airflow_api: str, authentication: HTTPBasicAuth) -> tuple[int, Any]:
    try:
        response = requests.get(airflow_api + f'/{dag_id}', auth=authentication)
        response_status, response_content = response.status_code, json.loads(response.text)
        return response_status, response_content
    except Exception as ex:
        print(f'Request if finding dag_id: {dag_id} failed , due to error: {ex}')
        raise Exception('request failed')


def delete_dag_by_id(response_content: dict, owner: str, authentication: HTTPBasicAuth, dag_id: str,
                     airflow_api: str) -> Response:
    if 'owners' in response_content and owner in response_content['owners']:
        del_response = requests.delete(airflow_api + f'/{dag_id}', auth=authentication)
        # remove from directory the .py file
        if del_response.status_code == 204:
            return make_response(jsonify({'message': f'dag:{dag_id} have been deleted'}), 200)
    else:
        return make_response(jsonify({'message': f'{owner} is not the owner of : {dag_id}'}), 403)
